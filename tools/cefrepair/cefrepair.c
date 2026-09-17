/*
 * cefrepair.c
 *
 *  ICNストリーミング配信における「修復アプリ」
 *
 *  ■このアプリの役割
 *      届いた映像チャンクの番号を見張り，飛んでいる番号があれば
 *      その番号だけを名指しで取り寄せる「番号の見張り番」
 *      映像データ本体は保存も再生もしない
 *
 *  ■置き場所と動かし方
 *      icn-router のサーバ上で動かす（同じマシンの cefnetd に接続する）
 *      例:  cefrepair ccnx:/video/stream1
 *
 *  ■動作の流れ（詳しくは cefrepair-dataflow.md のセクション4を参照）
 *      1. Symbolic Interest を送ってストリーミングコンテンツを要求する
 *      2. 届いたチャンクの番号を見て、飛び（欠損）を検出する
 *      3. 欠損した番号だけを Regular Interest で producer に取り寄せる
 *      4. 取り寄せた Data は cefnetd が自動で localcache に保存し，
 *         consumer にも転送してくれる（このアプリは何もしなくてよい）
 *
 *  ■責務の分離（このファイルが受け持つのは ④cefore I/O と ⑤配線 だけ）
 *      ① 欠損リスト管理   → repair_table.c / .h
 *      ② 欠損検出ロジック → loss_detect.c / .h
 *      ③ 修復スケジューラ → repair_sched.c / .h
 *      上の①②③は cefore に依存しない純粋ロジックなので単体テストできる．
 *      このファイルは，それらを cefnetd との送受信につなぎ込む役を担う．
 *
 *  ※このファイルは tools/cefgetstream/cefgetstream.c を雛形として作成．
 */

/****************************************************************************************
 Include Files（このアプリが使う外部の機能をまとめて読み込む）
 ****************************************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <limits.h>
#include <sys/time.h>
#include <stdarg.h>

/* cefore（ICN本体）が用意している機能を使うための宣言ファイル群 */
#include <cefore/cef_define.h>   /* 各種の定数（CefC_App_Version など）        */
#include <cefore/cef_frame.h>    /* パケット組み立て・Interest種別マクロ        */
#include <cefore/cef_client.h>   /* cefnetd と通信するための関数（接続・送受信） */
#include <cefore/cef_log.h>      /* ログ出力                                   */

/* このアプリの純粋ロジック（cefore非依存）を分離したモジュール群 */
#include "repair_table.h"        /* ① 欠損リスト（メモ②）の管理               */
#include "loss_detect.h"         /* ② 番号の見張り＝欠損検出（メモ①）          */
#include "repair_sched.h"        /* ③ 修復スケジューラ（場面B/C/D）            */

/****************************************************************************************
 Macros（このアプリの中で使う「設定値」に名前を付けておく）
 ****************************************************************************************/

/* エラーメッセージを画面に出すための便利な書き方 */
#define printerr(...)			fprintf(stderr,"[cefrepair] ERROR: " __VA_ARGS__)

/* 使い方（ヘルプ）を表示するための合言葉 */
#define USAGE					print_usage(stderr)

/* 修復の動作を決める設定値（CefC_Repair_Table_Size は repair_table.h、
   タイムアウト/再送回数/諦め境界は repair_sched.h で定義している）。 */

/****************************************************************************************
 State Variables（アプリ全体で共有する状態。プログラムが動いている間ずっと保持する）
 ****************************************************************************************/

/* メインループを回し続けるかどうかの旗。0になるとアプリが終了する。          */
static int app_running_f = 0;

/* cefnetd との接続を表す「取っ手」。送受信のたびにこれを使う。              */
static CefT_Client_Handle fhdl;

/****************************************************************************************
 Static Function Declaration（このファイルの中だけで使う関数の予告）
 ****************************************************************************************/
static void print_usage (FILE* ofp);
static void sigcatch (int sig);

/****************************************************************************************
 Main（プログラムはここから始まる）
 ****************************************************************************************/
int main (
	int argc,			/* コマンドに渡された言葉の数             */
	char** argv			/* コマンドに渡された言葉そのもの         */
) {
	int 	res;
	int 	i;
	char*	work_arg;

	char 	uri[1024];							/* 要求するコンテンツ名（URI）       */
	char 	conf_path[PATH_MAX] = {0};			/* 設定ファイルの置き場所            */
	int 	port_num = CefC_Unset_Port;			/* cefnetd のポート番号              */
	int		sg_lifetime = 4;					/* Symbolic Interest の寿命（秒）    */

	/* cefore に渡す「Interestの中身」を入れる箱。
	   sym=ストリーミングコンテンツ要求用(Symbolic)、reg=修復用(Regular) の2つを用意する。 */
	CefT_CcnMsg_OptHdr		opt;				/* オプション（寿命など）共通で使う  */
	CefT_CcnMsg_MsgBdy		params_sym;			/* Symbolic Interest の中身          */
	CefT_CcnMsg_MsgBdy		params_reg;			/* 修復用 Interest の中身            */

	/* 受信したパケットを一時的に置くバッファと、解釈結果を入れる箱 */
	unsigned char*			buff;
	struct cef_app_frame	app_frame;
	int						index = 0;			/* バッファに溜まった半端なデータ量  */

	/* 時刻を扱うための変数 */
	struct timeval	t;
	uint64_t		now_time;					/* 現在時刻（マイクロ秒）            */
	uint64_t		sym_resend_time;			/* 次にSymbolic Interestを送り直す時刻 */
	uint64_t		sym_resend_interval;		/* Symbolic Interestを送り直す間隔   */

	/* 分離したロジックモジュールが持つ状態（このファイルは配線するだけ） */
	CefT_Repair_Table		repair_table;		/* ① 欠損リスト本体                 */
	CefT_Loss_Detector		detector;			/* ② 番号の見張り状態（メモ①）      */
	CefT_Repair_Stats		stats;				/* 統計（各モジュールが加算する）    */
	CefT_Repair_SendList	send_list;			/* ③ が「送れ」と返してくる番号一覧  */

	/***** 入力チェック用の旗 *****/
	int uri_f = 0;

	/* 箱の中身を最初にすべて0で埋めておく（ゴミが残らないように） */
	memset (&opt,        0, sizeof (CefT_CcnMsg_OptHdr));
	memset (&params_sym, 0, sizeof (CefT_CcnMsg_MsgBdy));
	memset (&params_reg, 0, sizeof (CefT_CcnMsg_MsgBdy));
	memset (&stats,      0, sizeof (stats));
	repair_table_init (&repair_table);
	loss_detect_init  (&detector);
	uri[0] = 0;

	printf ("[cefrepair] Start\n");
	cef_log_init ("cefrepair", 1);

	/*---------------------------------------------------------------------------
		手順1: コマンドに渡されたオプションを読み取る
		  使い方:  cefrepair URI [-d 設定ディレクトリ] [-p ポート] [-z 寿命秒]
	-----------------------------------------------------------------------------*/
	for (i = 1 ; i < argc ; i++) {
		work_arg = argv[i];
		if (work_arg == NULL || work_arg[0] == 0) {
			break;
		}

		if (strcmp (work_arg, "-d") == 0) {
			/* -d : 設定ファイルの置き場所を指定する */
			if (i + 1 == argc) {
				printerr("[-d] has no parameter.\n");
				USAGE; return (-1);
			}
			if (strlen (argv[i + 1]) > PATH_MAX) {
				printerr("[-d] parameter is too long.\n");
				USAGE; return (-1);
			}
			strcpy (conf_path, argv[i + 1]);
			i++;
		} else if (strcmp (work_arg, "-p") == 0) {
			/* -p : cefnetd のポート番号を指定する */
			if (i + 1 == argc) {
				printerr("[-p] has no parameter.\n");
				USAGE; return (-1);
			}
			port_num = atoi (argv[i + 1]);
			i++;
		} else if (strcmp (work_arg, "-z") == 0) {
			/* -z : Symbolic Interest の寿命（秒）を指定する */
			if (i + 1 == argc) {
				printerr("[-z] has no parameter.\n");
				USAGE; return (-1);
			}
			sg_lifetime = atoi (argv[i + 1]);
			if (sg_lifetime < 1) {
				printerr("[-z] must be larger than 0.\n");
				USAGE; return (-1);
			}
			i++;
		} else if (strcmp (work_arg, "-h") == 0) {
			USAGE; exit (1);
		} else {
			/* オプションでない言葉は URI とみなす */
			if (work_arg[0] == '-') {
				printerr("unknown option is specified.\n");
				USAGE; return (-1);
			}
			if (uri_f) {
				printerr("uri is duplicated.\n");
				USAGE; return (-1);
			}
			if (strlen (work_arg) >= CefC_NAME_MAXLEN) {
				printerr("uri is too long.\n");
				USAGE; return (-1);
			}
			strcpy (uri, work_arg);
			uri_f++;
		}
	}

	/* URI が指定されていなければ何もできないので終了 */
	if (uri_f == 0) {
		printerr("uri is not specified.\n");
		USAGE; exit (1);
	}
	printf ("[cefrepair] Parsing parameters ... OK\n");
	cef_log_init2 (conf_path, 1);
#ifdef CefC_Debug
	cef_dbg_init ("cefrepair", conf_path, 1);
#endif // CefC_Debug

	/*---------------------------------------------------------------------------
		手順2: cefore のAPI（道具一式）を初期化し、cefnetd につなぐ
	-----------------------------------------------------------------------------*/
	cef_frame_init ();
	res = cef_client_init (port_num, conf_path);
	if (res < 0) {
		printerr("Failed to init the client package.\n");
		exit (1);
	}
	printf ("[cefrepair] Init Cefore Client package ... OK\n");

	/* 文字列のURI（例: ccnx:/video/stream1）を、cefore内部で使う
	   「Name」という形式に変換する。両方のInterestで同じ名前を使う。 */
	res = cef_frame_conversion_uri_to_name (uri, params_sym.name);
	if (res < 0) {
		printerr("Invalid URI is specified.\n");
		USAGE; exit (1);
	}
	params_sym.name_len = res;

	/* 同じ名前を修復用(reg)の箱にもコピーしておく */
	memcpy (params_reg.name, params_sym.name, params_sym.name_len);
	params_reg.name_len = params_sym.name_len;
	printf ("[cefrepair] Conversion from URI into Name ... OK\n");

	/* cefnetd に接続する。fhdl（取っ手）を以後ずっと使う。 */
	fhdl = cef_client_connect ();
	if (fhdl < 1) {
		printerr("cefnetd is not running.\n");
		exit (1);
	}
	printf ("[cefrepair] Connect to cefnetd ... OK\n");

	/* 受信用バッファを確保する */
	buff = (unsigned char*) malloc (sizeof (unsigned char) * CefC_AppBuff_Size);
	if (buff == NULL) {
		printerr("Failed to allocate the receive buffer.\n");
		exit (1);
	}
	memset (&app_frame, 0, sizeof (struct cef_app_frame));

	/*---------------------------------------------------------------------------
		手順3: 2種類の Interest の中身を準備する
	-----------------------------------------------------------------------------*/

	/* 共通オプション: 寿命を使うことを宣言 */
	opt.lifetime_f = 1;

	/* (A) Symbolic Interest（ストリーミングコンテンツを要求するために送る）:
	       Cef_Int_Symbolic マクロで「これはストリーミングコンテンツ要求だ」という旗を立てる。 */
	Cef_Int_Symbolic (params_sym);
	params_sym.hoplimit = 32;

	/* (B) 修復用 Regular Interest:
	       Cef_Int_Regular で「これは特定チャンクの要求だ」という旗を立て、
	       chunk_num_f=1 で「チャンク番号を指定する」と宣言しておく。
	       実際の番号(chunk_num)は、欠損が見つかるたびに毎回書き換える。 */
	Cef_Int_Regular (params_reg);
	params_reg.hoplimit  = 32;
	params_reg.chunk_num_f = 1;

	/*---------------------------------------------------------------------------
		手順4: 最初の Symbolic Interest を送る（ここからストリームが流れ始める）
	-----------------------------------------------------------------------------*/
	gettimeofday (&t, NULL);
	now_time = cef_client_covert_timeval_to_us (t);

	/* Symbolic Interestの寿命をオプションにセット（ミリ秒単位） */
	opt.lifetime = sg_lifetime * 1000;

	/* ストリーミングコンテンツ要求は寿命が切れる前に送り直して維持する。
	   寿命の80%が経過したら送り直すことにする。 */
	sym_resend_interval = (uint64_t)((double) opt.lifetime * 0.8) * 1000; /* us */
	sym_resend_time     = now_time + sym_resend_interval;

	app_running_f = 1;
	cef_client_interest_input (fhdl, &opt, &params_sym);
	printf ("[cefrepair] URI=%s\n", uri);
	printf ("[cefrepair] Start subscribing with Symbolic Interest\n");
	printf ("[cefrepair] Watching for lost chunks ... (Ctrl-C to stop)\n");

	/*===========================================================================
		手順5: メインループ
		  ここを延々と繰り返す。設計書セクション4-4の「ぐるぐる回る」部分。
		  受信パース(④)とInterest送信(④)はここに残し、番号の判断(②③)は
		  分離したモジュールに任せる。
	===========================================================================*/
	while (app_running_f) {

		/* Ctrl-C（中断）が押されたらループを抜ける準備をする */
		if (SIG_ERR == signal (SIGINT, sigcatch)) {
			break;
		}

		/* 今の時刻を取得しておく（タイムアウト判定などに使う） */
		gettimeofday (&t, NULL);
		now_time = cef_client_covert_timeval_to_us (t);

		/*-------------------------------------------------------------------
			【場面A】 cefnetd からチャンクを受け取り、番号をチェックする
			          （受信・パースは cefore I/O なのでここに残す。
			           番号の判断そのものは loss_detect に任せる。）
		-------------------------------------------------------------------*/
		res = cef_client_read (fhdl, &buff[index], CefC_AppBuff_Size - index);

		if (res > 0) {
			res += index;	/* 前回の半端な残り(index)と今回ぶんを合算 */

			/* バッファには複数のチャンクがまとめて入っていることがあるので、
			   1個ずつ取り出して処理する（do-while で繰り返す）。 */
			do {
				/* バッファから1チャンクぶんを解釈して app_frame に入れる */
				res = cef_client_payload_get_with_info (buff, res, &app_frame);

				/* 正しい cefore のデータでなければ、ここで打ち切る */
				if (app_frame.version != CefC_App_Version) {
					break;
				}

				/* Interest Return（要求が届かず戻ってきた通知）の場合は
				   今回は何もしない（修復アプリはストリーミングコンテンツ要求を続ける）。 */
				if ((uint8_t) app_frame.type == CefC_PT_INTRETURN) {
					continue;
				}

				/* ここまで来たら、1個の映像チャンクを受信できたということ。
				   映像本体（app_frame.payload）は保存せず捨てる。
				   使うのは「チャンク番号(app_frame.chunk_num)」だけ。 */
				stats.recv_chunks++;

				/* チャンク番号を持たないデータは番号の見張りができないので飛ばす */
				if (!app_frame.chunk_num_f) {
					continue;
				}

				/* ②へ委譲: 修復成功の消し込み・欠損の検出・最大番号の更新 */
				loss_detect_on_chunk (
					&detector, &repair_table, app_frame.chunk_num, &stats);

			} while (res > 0);

			/* 1チャンクに満たない半端なデータが残ったら、次回に持ち越す */
			index = (res > 0) ? res : 0;
		}

		/*-------------------------------------------------------------------
			【場面B/C/D】 欠損リストを見て、Regular Interest を送る／送り直す
			  どの番号を送るべきかの判断は ③(repair_sched) に任せ、
			  ここは返ってきた番号を実際に cefnetd へ送る役だけを担う。
		-------------------------------------------------------------------*/
		repair_sched_run (
			&repair_table, detector.max_seq_seen, now_time, &stats, &send_list);

		for (i = 0 ; i < send_list.count ; i++) {
			params_reg.chunk_num = send_list.chunks[i];
			opt.lifetime = CefC_Default_LifetimeSec * 1000;	/* 修復は通常寿命 */
			cef_client_interest_input (fhdl, &opt, &params_reg);
		}

		/*-------------------------------------------------------------------
			ストリーミングコンテンツ要求の維持: 寿命が切れる前に Symbolic Interest を送り直す
		-------------------------------------------------------------------*/
		if (now_time > sym_resend_time) {
			opt.lifetime = sg_lifetime * 1000;	/* ストリーミングコンテンツ要求用の寿命に戻す */
			cef_client_interest_input (fhdl, &opt, &params_sym);
			sym_resend_time = now_time + sym_resend_interval;
		}
	}

	/*---------------------------------------------------------------------------
		手順6: 後片付け（ストリーミングコンテンツ要求を止め、接続を閉じ、成果を表示する）
	-----------------------------------------------------------------------------*/
	printf ("\n[cefrepair] Stopping ...\n");

	/* 寿命0の Symbolic Interest を送ると「ストリーミングコンテンツ要求をやめる」合図になる */
	opt.lifetime = 0;
	cef_client_interest_input (fhdl, &opt, &params_sym);

	usleep (1000000);	/* 後始末が伝わるよう少し待つ */
	cef_client_close (fhdl);
	free (buff);

	printf ("[cefrepair] ===== Statistics =====\n");
	printf ("[cefrepair] Received chunks    = "FMTU64"\n", stats.recv_chunks);
	printf ("[cefrepair] Losses detected    = "FMTU64"\n", stats.loss_detected);
	printf ("[cefrepair] Repaired (arrived) = "FMTU64"\n", stats.repaired);
	printf ("[cefrepair] Regular Interests  = "FMTU64"\n", stats.regular_sent);
	printf ("[cefrepair] Gave up            = "FMTU64"\n", stats.gaveup);
	printf ("[cefrepair] Terminate\n");

	exit (0);
}

/****************************************************************************************
 その他の補助関数
 ****************************************************************************************/

/*
 * Ctrl-C（SIGINT）が押されたときに呼ばれる。
 *   メインループの旗を下ろして、安全に終了へ向かわせる。
 */
static void
sigcatch (
	int sig
) {
	if (sig == SIGINT) {
		app_running_f = 0;
	}
}

/*
 * 使い方（ヘルプ）を表示する。
 */
static void
print_usage (
	FILE* ofp
) {
	fprintf (ofp, "\nUsage: cefrepair\n\n");
	fprintf (ofp, "  cefrepair uri [-d config_file_dir] [-p port_num] [-z Lifetime]\n\n");
	fprintf (ofp, "  uri              Specify the URI. (e.g. ccnx:/video/stream1)\n");
	fprintf (ofp, "  config_file_dir  Configure file directory\n");
	fprintf (ofp, "  port_num         Port Number\n");
	fprintf (ofp, "  Lifetime         Symbolic Interest Lifetime (sec)\n\n");
}
