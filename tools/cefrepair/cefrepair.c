/*
 * cefrepair.c
 *
 *  ICNストリーミング配信における「修復アプリ」。
 *
 *  ■このアプリの役割（一言でいうと）
 *      届いた映像チャンクの番号を見張り、飛んでいる番号があったら、
 *      その番号だけを名指しで取り寄せる「番号の見張り番」。
 *      映像データ本体は保存も再生もしない。
 *
 *  ■置き場所と動かし方
 *      icn-router のサーバ上で動かす（同じマシンの cefnetd に接続する）。
 *      例:  cefrepair ccnx:/video/stream1
 *
 *  ■動作の流れ（詳しくは cefrepair-dataflow.md のセクション4を参照）
 *      1. Symbolic Interest を送ってストリーミングコンテンツを要求する
 *      2. 届いたチャンクの番号を見て、飛び（欠損）を検出する
 *      3. 欠損した番号だけを Regular Interest で producer に取り寄せる
 *      4. 取り寄せた Data は cefnetd が自動で localcache に保存し、
 *         consumer にも転送してくれる（このアプリは何もしなくてよい）
 *
 *  ※このファイルは tools/cefgetstream/cefgetstream.c を雛形として作成した。
 */

#define __CEF_REPAIR_SOURECE__

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

/****************************************************************************************
 Macros（このアプリの中で使う「設定値」に名前を付けておく）
 ****************************************************************************************/

/* エラーメッセージを画面に出すための便利な書き方 */
#define printerr(...)			fprintf(stderr,"[cefrepair] ERROR: " __VA_ARGS__)

/* 使い方（ヘルプ）を表示するための合言葉 */
#define USAGE					print_usage(stderr)

/*--- 修復の動作を決める設定値（実験に合わせて調整する） -----------------------------*/

/* 欠損リスト（メモ②）の最大行数。これ以上の欠損は同時に管理できない。       */
#define CefC_Repair_Table_Size		4096

/* Regular Interest を送ってから「返事が来ない」と判断するまでの待ち時間。    */
/* 単位はマイクロ秒。100000マイクロ秒 = 100ミリ秒。                          */
#define CefC_Repair_Timeout_us		100000

/* 1つの欠損チャンクを最大何回まで注文し直すか（無限ループ防止）。           */
#define CefC_Repair_Max_Retry		3

/* 「これより古い番号はもう諦める」境界の余裕（チャンク数）。               */
/* 最新番号からこの数だけ過去までは取り寄せを試みる。cefnetd.conf の        */
/* SYMBOLIC_BACKBUFFER（既定100）より少し小さい値にしておくのが安全。       */
#define CefC_Repair_GiveUp_Margin	80

/****************************************************************************************
 Structures Declaration（自分で作るデータの「形」を定義する）
 ****************************************************************************************/

/*
 * 欠損リスト（メモ②）の1行ぶんを表す箱。
 * 「どの番号が」「いつ注文されて」「何回注文したか」を覚えておく。
 */
typedef struct {
	int			used;			/* この行が使用中か（1=使用中, 0=空き）         */
	uint32_t	chunk_num;		/* 欠損しているチャンク番号                    */
	uint64_t	last_req_time;	/* 最後に Regular Interest を送った時刻(us)     */
	int			retry_count;	/* これまでに注文した回数                      */
} CefT_Repair_Entry;

/****************************************************************************************
 State Variables（アプリ全体で共有する状態。プログラムが動いている間ずっと保持する）
 ****************************************************************************************/

/* メインループを回し続けるかどうかの旗。0になるとアプリが終了する。          */
static int app_running_f = 0;

/* cefnetd との接続を表す「取っ手」。送受信のたびにこれを使う。              */
static CefT_Client_Handle fhdl;

/* 欠損リスト（メモ②）本体。CefT_Repair_Table_Size 行ぶんの配列。           */
static CefT_Repair_Entry repair_table[CefC_Repair_Table_Size];

/*--- 統計（最後に成果を表示するための数え上げ） -------------------------------------*/
static uint64_t stat_recv_chunks   = 0;	/* 受信したチャンク数               */
static uint64_t stat_loss_detected = 0;	/* 欠損として検出した数             */
static uint64_t stat_repaired      = 0;	/* 修復に成功した（届いた）数       */
static uint64_t stat_regular_sent  = 0;	/* 送った Regular Interest の総数   */
static uint64_t stat_gaveup        = 0;	/* 諦めた数                         */

/****************************************************************************************
 Static Function Declaration（このファイルの中だけで使う関数の予告）
 ****************************************************************************************/
static void print_usage (FILE* ofp);
static void sigcatch (int sig);

/* 欠損リスト（メモ②）を操作するための小さな関数たち */
static void repair_table_add    (uint32_t chunk_num);	/* 1件追加               */
static void repair_table_remove (uint32_t chunk_num);	/* 1件削除（修復成功時） */
static int  repair_table_find   (uint32_t chunk_num);	/* 何行目にあるか探す     */

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

	/* 番号の見張りに使う変数（メモ①にあたる） */
	int			first_received_f = 0;			/* 最初のチャンクを受信済みか        */
	uint32_t	max_seq_seen = 0;				/* これまでに見た最大のチャンク番号  */

	/***** 入力チェック用の旗 *****/
	int uri_f = 0;

	/* 箱の中身を最初にすべて0で埋めておく（ゴミが残らないように） */
	memset (&opt,        0, sizeof (CefT_CcnMsg_OptHdr));
	memset (&params_sym, 0, sizeof (CefT_CcnMsg_MsgBdy));
	memset (&params_reg, 0, sizeof (CefT_CcnMsg_MsgBdy));
	memset (repair_table, 0, sizeof (repair_table));
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
				stat_recv_chunks++;

				/* チャンク番号を持たないデータは番号の見張りができないので飛ばす */
				if (!app_frame.chunk_num_f) {
					continue;
				}

				uint32_t seq = app_frame.chunk_num;

				/* --- A-1: いちばん最初に受け取ったチャンクの場合 --- */
				if (!first_received_f) {
					/* ストリームの途中からストリーミングコンテンツ要求を始めたので、これより前の
					   番号は「欠損」ではない。ここを起点にするだけ。 */
					first_received_f = 1;
					max_seq_seen = seq;
					continue;
				}

				/* --- A-2: 欠損リストに載っていた番号が今届いた場合 --- */
				/*         → 修復成功。リストから消す。                  */
				if (repair_table_find (seq) >= 0) {
					repair_table_remove (seq);
					stat_repaired++;
					printf ("[cefrepair] Repaired chunk=%u\n", seq);
				}

				/* --- A-3: 番号が飛んでいた場合（欠損の検出） --- */
				/*         例: これまで最大5なのに、8が来た → 6,7が欠損  */
				if (seq > max_seq_seen + 1) {
					uint32_t k;
					for (k = max_seq_seen + 1 ; k < seq ; k++) {
						repair_table_add (k);
						stat_loss_detected++;
						printf ("[cefrepair] Detected loss chunk=%u\n", k);
					}
				}

				/* --- A-4: これまでの最大番号を更新する（メモ①） --- */
				if (seq > max_seq_seen) {
					max_seq_seen = seq;
				}

			} while (res > 0);

			/* 1チャンクに満たない半端なデータが残ったら、次回に持ち越す */
			index = (res > 0) ? res : 0;
		}

		/*-------------------------------------------------------------------
			【場面B/C】 欠損リストを見て、Regular Interest を送る／送り直す
			  ・まだ注文していない行 → 初めての注文（場面B）
			  ・注文済みだが返事が遅い行 → 注文し直す（場面C）
			  ・諦め判定（場面D）も同じループの中で行う
		-------------------------------------------------------------------*/
		for (i = 0 ; i < CefC_Repair_Table_Size ; i++) {

			/* 空き行は飛ばす */
			if (!repair_table[i].used) {
				continue;
			}

			/* 【場面D】 古すぎる番号はもう間に合わないので諦める。
			   最新番号から CefC_Repair_GiveUp_Margin より過去なら削除。 */
			if (max_seq_seen > CefC_Repair_GiveUp_Margin &&
				repair_table[i].chunk_num < max_seq_seen - CefC_Repair_GiveUp_Margin) {
				printf ("[cefrepair] Give up chunk=%u (too old)\n",
						repair_table[i].chunk_num);
				repair_table[i].used = 0;
				stat_gaveup++;
				continue;
			}

			/* 【場面B】 まだ一度も注文していない行（retry_count==0） */
			if (repair_table[i].retry_count == 0) {
				/* この行の番号を、修復用Interestの箱にセットして送る */
				params_reg.chunk_num = repair_table[i].chunk_num;
				opt.lifetime = CefC_Default_LifetimeSec * 1000; /* 修復は通常寿命 */
				cef_client_interest_input (fhdl, &opt, &params_reg);

				/* 「注文した時刻」と「1回目」を記録する */
				repair_table[i].last_req_time = now_time;
				repair_table[i].retry_count   = 1;
				stat_regular_sent++;
				printf ("[cefrepair] Request(1st) chunk=%u\n", repair_table[i].chunk_num);
				continue;
			}

			/* 【場面C】 注文済みだが、待ち時間を過ぎても返事が来ない行 */
			if (now_time - repair_table[i].last_req_time > CefC_Repair_Timeout_us) {

				if (repair_table[i].retry_count >= CefC_Repair_Max_Retry) {
					/* 上限まで注文したのに届かない → 諦めて削除 */
					printf ("[cefrepair] Give up chunk=%u (max retry)\n",
							repair_table[i].chunk_num);
					repair_table[i].used = 0;
					stat_gaveup++;
				} else {
					/* もう一度注文し直し、回数を1つ増やす */
					params_reg.chunk_num = repair_table[i].chunk_num;
					opt.lifetime = CefC_Default_LifetimeSec * 1000;
					cef_client_interest_input (fhdl, &opt, &params_reg);

					repair_table[i].last_req_time = now_time;
					repair_table[i].retry_count++;
					stat_regular_sent++;
					printf ("[cefrepair] Request(retry %d) chunk=%u\n",
							repair_table[i].retry_count, repair_table[i].chunk_num);
				}
			}
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
	printf ("[cefrepair] Received chunks    = "FMTU64"\n", stat_recv_chunks);
	printf ("[cefrepair] Losses detected    = "FMTU64"\n", stat_loss_detected);
	printf ("[cefrepair] Repaired (arrived) = "FMTU64"\n", stat_repaired);
	printf ("[cefrepair] Regular Interests  = "FMTU64"\n", stat_regular_sent);
	printf ("[cefrepair] Gave up            = "FMTU64"\n", stat_gaveup);
	printf ("[cefrepair] Terminate\n");

	exit (0);
}

/****************************************************************************************
 欠損リスト（メモ②）を操作する関数たち
 ****************************************************************************************/

/*
 * 欠損リストに番号を1件追加する。
 *   すでに同じ番号があれば二重登録はしない。
 *   空き行がなければ（リストが満杯なら）警告だけ出して諦める。
 */
static void
repair_table_add (
	uint32_t chunk_num
) {
	int i;

	/* すでに登録済みなら何もしない */
	if (repair_table_find (chunk_num) >= 0) {
		return;
	}

	/* 空いている行を探して、そこに書き込む */
	for (i = 0 ; i < CefC_Repair_Table_Size ; i++) {
		if (!repair_table[i].used) {
			repair_table[i].used          = 1;
			repair_table[i].chunk_num     = chunk_num;
			repair_table[i].last_req_time = 0;
			repair_table[i].retry_count   = 0;	/* まだ注文していない印 */
			return;
		}
	}

	/* ここに来たら満杯。実験では表サイズを増やすかタイムアウトを短くする。 */
	printerr("repair table is full (chunk=%u dropped)\n", chunk_num);
}

/*
 * 欠損リストから指定番号の行を削除する（修復成功時に呼ぶ）。
 */
static void
repair_table_remove (
	uint32_t chunk_num
) {
	int idx = repair_table_find (chunk_num);
	if (idx >= 0) {
		repair_table[idx].used = 0;	/* 使用中の旗を下ろせば「空き」になる */
	}
}

/*
 * 欠損リストの中から指定番号を探す。
 *   見つかれば「何行目か(0以上)」を、なければ -1 を返す。
 */
static int
repair_table_find (
	uint32_t chunk_num
) {
	int i;
	for (i = 0 ; i < CefC_Repair_Table_Size ; i++) {
		if (repair_table[i].used && repair_table[i].chunk_num == chunk_num) {
			return (i);
		}
	}
	return (-1);
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
