/*
 * Copyright (c) 2016-2023, National Institute of Information and Communications
 * Technology (NICT). All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the NICT nor the names of its contributors may be
 *    used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE NICT AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE NICT OR CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
/*
 * cefgetstream.c
 */

#define __CEF_GETFILE_SOURECE__

/****************************************************************************************
 Include Files
 ****************************************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <signal.h>
#include <limits.h>
#include <sys/time.h>
#include <stdarg.h>

#include <cefore/cef_define.h>
#include <cefore/cef_frame.h>
#include <cefore/cef_client.h>
#include <cefore/cef_valid.h>
#include <cefore/cef_log.h>

/* 欠損検知＋再要求の頭脳ロジック（cefore非依存。cefrepair と同じ実装を共有）。
   Symbolic モード受信時のラストホップ欠損を検出し Regular Interest で再要求する。 */
#include "repair_table.h"		/* 欠損リスト（メモ②）の管理       */
#include "loss_detect.h"		/* 番号の見張り＝欠損検出（メモ①）  */
#include "repair_sched.h"		/* 修復スケジューラ（場面B/C/D）    */
#include "reorder_buf.h"		/* 整列バッファ（in-order 出力）    */

#include <fcntl.h>			/* O_NONBLOCK / F_GETFL（NONBLOCK出力用） */

/****************************************************************************************
 Macros
 ****************************************************************************************/

#define CefC_Max_PipeLine 		1024	/* MAX Pipeline */
#define CefC_Def_PipeLine 		8		/* Default Pipeline */

#define USAGE					print_usage(CefFp_Usage)
#define printerr(...)			fprintf(stderr,"[cefgetstream] ERROR: " __VA_ARGS__)

/****************************************************************************************
 Structures Declaration
 ****************************************************************************************/

typedef struct _Ceft_RxWnd {
	
	uint64_t 				seq;
	uint8_t 				flag;
	unsigned char 			buff[CefC_Max_Length];
	int 					frame_size;
	struct _Ceft_RxWnd* 	next;
	
} Ceft_RxWnd;

/****************************************************************************************
 State Variables
 ****************************************************************************************/

static int app_running_f = 0;

static uint64_t stat_recv_frames = 0;
static uint64_t stat_recv_bytes = 0;
static uint64_t stat_jitter_sum = 0;
static uint64_t stat_jitter_sq_sum = 0;
static uint64_t stat_jitter_max = 0;
static struct timeval start_t;
static struct timeval end_t;
CefT_Client_Handle fhdl;

/* --- トレース計装（実時間性の評価用）--------------------------------------
   チャンク1個につき1行、「いつ届いて・いつ欠損を検出して・いつ出力したか」を
   CSV で書き出す。ここから 修復遅延／整列滞留時間／出力ギャップ(=フリーズ)／
   間に合い率 を後処理で計算する。--trace <path> を付けたときだけ有効。
   映像は stdout 専用なので、トレースは必ずファイルへ書く（stderr も統計表示
   で使うため混ぜない）。 */
static FILE*	g_trace_fp		= NULL;	/* トレース出力先（NULL=無効） */
static uint64_t	g_trace_t0		= 0;	/* 相対時刻の起点(us)          */
static uint64_t	g_trace_lines	= 0;	/* 書き出した行数              */

/****************************************************************************************
 Static Function Declaration
 ****************************************************************************************/

static void
post_process (
	FILE* ofp
);
static void
sigcatch (
	int sig
);
static void
print_usage (
	FILE* ofp
);
/* 整列バッファから in-order に出せる分を取り出して stdout へ書き出す。
   block/nonblock の出力切替は元の Symbolic 受信と同じ挙動を保つ。 */
static void
drain_reorder (
	CefT_Reorder_Buf*	rb,
	uint32_t			max_seq_seen,
	uint32_t			give_up_margin,
	int					blk_mode_val,
	int*				first_out_f,
	uint64_t			now_time
);
/* トレースに1行書く（--trace 指定時のみ）。時刻は起点からの相対 us。 */
static void
trace_write (
	uint32_t				seq,
	const CefT_Chunk_Meta*	meta,
	uint64_t				t_out,
	int						len
);

/****************************************************************************************
 ****************************************************************************************/
int main (
	int argc,
	char** argv
) {
	int res;
	int pipeline = CefC_Def_PipeLine;
	int index = 0;
	char uri[1024];
	CefT_CcnMsg_OptHdr opt;
	CefT_CcnMsg_MsgBdy params;
	struct timeval t;
	uint64_t dif_time;
	uint64_t nxt_time;
	uint64_t now_time;
	uint64_t end_time;
	uint64_t val;
	uint64_t diff_seq;
	int send_cnt = 0;
	int i;
	char*	work_arg;
	
	char 	conf_path[PATH_MAX] = {0};
	int 	port_num = CefC_Unset_Port;
	
	char valid_type[1024];
	
	struct cef_app_frame app_frame;
	unsigned char* buff;
	
	Ceft_RxWnd* 	rxwnd;
	Ceft_RxWnd* 	rxwnd_prev;
	Ceft_RxWnd* 	rxwnd_head;
	Ceft_RxWnd* 	rxwnd_tail;

	int backup_fd;

	/* 修復用 Regular Interest と、欠損検知＋再要求の状態（Symbolicモードでのみ使う） */
	CefT_CcnMsg_MsgBdy		params_reg;		/* 再要求する特定チャンク用 Interest */
	CefT_Repair_Table		repair_table;	/* 欠損リスト本体（メモ②）          */
	CefT_Loss_Detector		detector;		/* 番号の見張り状態（メモ①）        */
	CefT_Repair_Stats		repair_stats;	/* 修復の統計                       */
	CefT_Repair_SendList	send_list;		/* 「今送るべき番号」の一覧         */
	CefT_Reorder_Buf		reorder;		/* 整列バッファ（in-order 出力）    */
	int						first_out_f = 0;/* 最初の出力を行ったか(NONBLOCK設定用) */
	CefT_Loss_Result		loss_res;		/* 1チャンク処理の結果（トレース用）*/
	CefT_Chunk_Meta			chunk_meta;		/* 整列バッファへ随伴させるメタ情報 */

	/* 諦め境界。既定は CefC_Repair_GiveUp_Margin(80)。実験で振るため
	   --giveup-margin で実行時に変更できる（再ビルド不要）。 */
	uint32_t	giveup_margin		= CefC_Repair_GiveUp_Margin;
	char		trace_path[PATH_MAX] = {0};

	/* 先頭保護のチャンク数（0 で無効）。--head-protect で変更できる。
	   ストリーム先頭のこの範囲（mp4 なら ftyp と moov＝索引）は、諦め境界を
	   窓の上限まで延ばし、再要求の回数上限も外して修復を待つ。
	   head_limit は「最初に受信した番号 + head_protect」で、最初のチャンクを
	   受けた時点で確定する。 */
	uint32_t	head_protect		= CefC_Reorder_Head_Protect;
	uint32_t	head_limit			= 0;

	/***** flags 		*****/
	int pipeline_f 		= 0;
	int max_seq_f 		= 0;
	int uri_f 			= 0;
	int nsg_flag 		= 0;
	int from_pub_f 		= 0;
	int dir_path_f 		= 0;
	int port_num_f 		= 0;
	int valid_f 		= 0;
	//0.8.3
	int blk_mode_f		= 0;
	int blk_mode_val	= 0;	//BLOCK
	int trace_f			= 0;
	int giveup_f		= 0;
	int head_f			= 0;
	
	/***** state variavles 	*****/
	uint32_t 	sv_max_seq 		= UINT_MAX - 1;
	int			sg_lifetime		= 4;
	
	memset (&opt, 0, sizeof (CefT_CcnMsg_OptHdr));
	memset (&params, 0, sizeof (CefT_CcnMsg_MsgBdy));
	memset (&params_reg, 0, sizeof (CefT_CcnMsg_MsgBdy));
	memset (&repair_stats, 0, sizeof (repair_stats));
	memset (&reorder, 0, sizeof (reorder));	/* slots=NULL にしておく（destroy安全化） */
	repair_table_init (&repair_table);
	loss_detect_init  (&detector);

	
	/*---------------------------------------------------------------------------
		Obtains parameters
	-----------------------------------------------------------------------------*/
	uri[0] 			= 0;
	valid_type[0] 	= 0;

	/* 早期に stdout(fd1) を stderr へ退避する。これ以降の起動表示・ログ・
	   デバッグ出力はすべて stderr に出て、stdout(=映像)を汚さない。
	   メインループ直前に dup2(backup_fd,1) で本来の stdout に戻す。 */
	backup_fd = dup (1);
	dup2 (2, 1);

	printf ("[cefgetstream] Start\n");
	
	/* Inits logging 		*/
	cef_log_init ("cefgetstream", 1);
	
	/* Obtains options 		*/
	for (i = 1 ; i < argc ; i++) {
		
		work_arg = argv[i];
		if (work_arg == NULL || work_arg[0] == 0) {
			break;
		}
		
		if (strcmp (work_arg, "-s") == 0) {
			if (pipeline_f) {
				printerr("[-s] is duplicated.\n");
				USAGE;
				return (-1);
			}
			if (i + 1 == argc) {
				printerr("[-s] has no parameter.\n");
				USAGE;
				return (-1);
			}
			work_arg = argv[i + 1];
			pipeline = atoi (work_arg);
//			if ((pipeline < 1) || (pipeline > CefC_Max_PileLine)) {
//				pipeline = 1;
//			}
			if ( pipeline < 1 ) {
				pipeline = CefC_Def_PipeLine;
			} else if ( pipeline > CefC_Max_PipeLine ) {
				pipeline = CefC_Max_PipeLine;
			}
			pipeline_f++;
			i++;
		} else if (strcmp (work_arg, "-d") == 0) {
			if (dir_path_f) {
				printerr("[-d] is duplicated.\n");
				USAGE;
				return (-1);
			}
			if (i + 1 == argc) {
				printerr("[-d] has no parameter.\n");
				USAGE;
				return (-1);
			}
			//202108
			if (strlen(argv[i + 1]) > PATH_MAX) {
				printerr("[-d] parameter is too long.\n");
				USAGE;
				return (-1);
			}
			work_arg = argv[i + 1];
			strcpy (conf_path, work_arg);
			dir_path_f++;
			i++;
		} else if (strcmp (work_arg, "-p") == 0) {
			if (port_num_f) {
				printerr("[-p] is duplicated.\n");
				USAGE;
				return (-1);
			}
			if (i + 1 == argc) {
				printerr("[-p] has no parameter.\n");
				USAGE;
				return (-1);
			}
			work_arg = argv[i + 1];
			port_num = atoi (work_arg);
			port_num_f++;
			i++;
		//0.8.3
		} else if (strcmp (work_arg, "-l") == 0) {
			if (port_num_f) {
				printerr("[-l] is duplicated.\n");
				USAGE;
				return (-1);
			}
			if (i + 1 == argc) {
				printerr("[-l] has no parameter.\n");
				USAGE;
				return (-1);
			}
			work_arg = argv[i + 1];
			blk_mode_val = atoi (work_arg);
			if ( (blk_mode_val == 0) || (blk_mode_val == 1) ) {
				/* OK */
			} else {
				printerr("block_mode is 0 or 1.\n");
				USAGE;
				return (-1);
			}
			blk_mode_f++;
			i++;
		} else if (strcmp (work_arg, "-m") == 0) {
			if (max_seq_f) {
				printerr("[-m] is duplicated.\n");
				USAGE;
				return (-1);
			}
			if (i + 1 == argc) {
				printerr("[-m] has no parameter.\n");
				USAGE;
				return (-1);
			}
			work_arg = argv[i + 1];
			sv_max_seq = (uint32_t) atoi (work_arg);
			
			if (sv_max_seq < 1) {
				sv_max_seq = 1;
			}
			sv_max_seq--;
			max_seq_f++;
			i++;
		} else if (strcmp (work_arg, "-z") == 0) {
			if (nsg_flag) {
				printerr("[-z] is duplicated.\n");
				USAGE;
				return (-1);
			}
			if (i + 1 == argc) {
				
			} else {
				work_arg = argv[i + 1];
				sg_lifetime = atoi (work_arg);
				if ( sg_lifetime < 0 ) {
					printerr("[-z] has the invalid parameter.(Lifetime > 0)\n");
					USAGE;
					return(-1);
				}
			}
			nsg_flag++;
			i++;
		} else if (strcmp (work_arg, "-o") == 0) {
			if (from_pub_f) {
				printerr("[-o] is duplicated.\n");
				USAGE;
				return (-1);
			}
			from_pub_f++;
		} else if (strcmp (work_arg, "-v") == 0) {
			if (valid_f) {
				printerr("[-v] is duplicated.\n");
				USAGE;
				return (-1);
			}
			if (i + 1 == argc) {
				printerr("[-v] has no parameter.\n");
				USAGE;
				return (-1);
			}
			work_arg = argv[i + 1];
			strcpy (valid_type, work_arg);
			valid_f++;
			i++;
		} else if (strcmp (work_arg, "--trace") == 0) {
			/* チャンク単位のトレースを CSV で書き出す（実時間性の評価用） */
			if (trace_f) {
				printerr("[--trace] is duplicated.\n");
				USAGE;
				return (-1);
			}
			if (i + 1 == argc) {
				printerr("[--trace] has no parameter.\n");
				USAGE;
				return (-1);
			}
			work_arg = argv[i + 1];
			if (strlen (work_arg) >= PATH_MAX) {
				printerr("[--trace] path is too long.\n");
				USAGE;
				return (-1);
			}
			strcpy (trace_path, work_arg);
			trace_f++;
			i++;
		} else if (strcmp (work_arg, "--giveup-margin") == 0) {
			/* 諦め境界（チャンク数）を実行時に指定する。実験で振るため。 */
			if (giveup_f) {
				printerr("[--giveup-margin] is duplicated.\n");
				USAGE;
				return (-1);
			}
			if (i + 1 == argc) {
				printerr("[--giveup-margin] has no parameter.\n");
				USAGE;
				return (-1);
			}
			work_arg = argv[i + 1];
			res = atoi (work_arg);
			/* 窓より小さく、起点確定の猶予より大きい必要がある
			   （窓 > 境界 > Start_Grace）。 */
			if (res <= CefC_Reorder_Start_Grace || res >= CefC_Reorder_Window) {
				printerr("[--giveup-margin] must be > %d and < %d.\n",
					CefC_Reorder_Start_Grace, CefC_Reorder_Window);
				USAGE;
				return (-1);
			}
			giveup_margin = (uint32_t) res;
			giveup_f++;
			i++;
		} else if (strcmp (work_arg, "--head-protect") == 0) {
			/* 先頭保護のチャンク数を指定する（0 で無効＝比較用）。 */
			if (head_f) {
				printerr("[--head-protect] is duplicated.\n");
				USAGE;
				return (-1);
			}
			if (i + 1 == argc) {
				printerr("[--head-protect] has no parameter.\n");
				USAGE;
				return (-1);
			}
			work_arg = argv[i + 1];
			res = atoi (work_arg);
			if (res < 0) {
				printerr("[--head-protect] must be >= 0.\n");
				USAGE;
				return (-1);
			}
			head_protect = (uint32_t) res;
			head_f++;
			i++;
		} else if (strcmp (work_arg, "-h") == 0) {
			USAGE;
			exit (1);
		} else {
			
			work_arg = argv[i];
			
			if (work_arg[0] == '-') {
				printerr("unknown option is specified.\n");
				USAGE;
				return (-1);
			}
			
			if (uri_f) {
				printerr("uri is duplicated.\n");
				USAGE;
				return (-1);
			}
			res = strlen (work_arg);
			
			if (res >= CefC_NAME_MAXLEN) {
				printerr("uri is too long.\n");
				USAGE;
				return (-1);
			}
			strcpy (uri, work_arg);
			uri_f++;
		}
	}
	
	/* Checks errors 			*/
	if (uri_f == 0) {
		printerr("uri is not specified.\n");
		USAGE;
		exit (1);
	}
	if (pipeline > sv_max_seq + 1) {
		pipeline = sv_max_seq + 1;
	}
	printf ("[cefgetstream] Parsing parameters ... OK\n");
	cef_log_init2 (conf_path, 1 /* for CEFNETD */);
#ifdef CefC_Debug
	cef_dbg_init ("cefgetstream", conf_path, 1);
#endif // CefC_Debug
	
	/*---------------------------------------------------------------------------
		Inits the Cefore APIs
	-----------------------------------------------------------------------------*/
	cef_frame_init ();
	res = cef_client_init (port_num, conf_path);
	if (res < 0) {
		printerr("Failed to init the client package.\n");
		exit (1);
	}
	printf ("[cefgetstream] Init Cefore Client package ... OK\n");
	res = cef_frame_conversion_uri_to_name (uri, params.name);
	if (res < 0) {
		printerr("Invalid URI is specified.\n");
		USAGE;
		exit (1);
	}
	printf ("[cefgetstream] Conversion from URI into Name ... OK\n");
	params.name_len = res;
	printf ("[cefgetstream] Checking the output file ... OK\n");
	
	/*------------------------------------------
		Set Validation Alglithm
	--------------------------------------------*/
	if (valid_f == 1) {
		cef_valid_init (conf_path);
		params.alg.valid_type = (uint16_t) cef_valid_type_get (valid_type);
		
		if (params.alg.valid_type == CefC_T_ALG_INVALID) {
			printerr("-v has the invalid parameter %s\n", valid_type);
			exit (1);
		}
	}
	
	/*------------------------------------------
		Connects to CEFORE
	--------------------------------------------*/
	fhdl = cef_client_connect ();
	if (fhdl < 1) {
		printerr("cefnetd is not running.\n");
		exit (1);
	}
	printf ("[cefgetstream] Connect to cefnetd ... OK\n");
	buff = (unsigned char*) malloc (sizeof (unsigned char) * CefC_AppBuff_Size);
	memset (&app_frame, 0, sizeof (struct cef_app_frame));
	
	/*---------------------------------------------------------------------------
		Sets Interest parameters
	-----------------------------------------------------------------------------*/
	params.hoplimit 				= 32;
	opt.lifetime_f 			= 1;
	
	if (nsg_flag) {
		Cef_Int_Symbolic(params);
		opt.lifetime 		= sg_lifetime * 1000;	//0.8.3

		/* 修復用 Regular Interest を準備する。Symbolic と同じ名前を使い、
		   chunk_num_f=1 で「特定チャンク番号を指定する」と宣言しておく。
		   実際の番号は欠損が見つかるたびに params_reg.chunk_num を書き換える。 */
		memcpy (params_reg.name, params.name, params.name_len);
		params_reg.name_len    = params.name_len;
		Cef_Int_Regular (params_reg);
		params_reg.hoplimit    = 32;
		params_reg.chunk_num_f = 1;
		if (from_pub_f) {
			params_reg.org.from_pub_f = CefC_T_FROM_PUB;
		}

		/* 整列バッファを確保する（Symbolicモードでのみ使う） */
		if (reorder_init (&reorder) < 0) {
			printerr("Failed to allocate the reorder buffer.\n");
			exit (1);
		}

		/* トレース出力を開く（--trace 指定時のみ）。先頭に条件を注記して
		   おくと、後処理でどの設定のランか分かる。 */
		if (trace_f) {
			g_trace_fp = fopen (trace_path, "w");
			if (g_trace_fp == NULL) {
				printerr("Failed to open the trace file: %s\n", trace_path);
				exit (1);
			}
			gettimeofday (&t, NULL);
			g_trace_t0 = cef_client_covert_timeval_to_us (t);
			fprintf (g_trace_fp,
				"# cefgetstream trace\n"
				"# uri=%s\n"
				"# start_epoch_us=" FMTU64 "\n"
				"# giveup_margin=%u\n"
				"# head_protect=%u\n"
				"# head_margin=%d\n"
				"# reorder_window=%d\n"
				"# start_grace=%d\n"
				"# repair_timeout_us=%d\n"
				"# repair_max_retry=%d\n"
				"# sg_lifetime_sec=%d\n"
				"# times are microseconds relative to start_epoch_us\n"
				"seq,t_arrive_us,t_detect_us,t_out_us,n_req,kind,len\n",
				uri, g_trace_t0, giveup_margin,
				head_protect, CefC_Reorder_Head_Margin,
				CefC_Reorder_Window, CefC_Reorder_Start_Grace,
				CefC_Repair_Timeout_us, CefC_Repair_Max_Retry, sg_lifetime);
			fprintf (stderr, "[cefgetstream] Trace  = %s (giveup_margin=%u)\n",
				trace_path, giveup_margin);
		}
	} else {
		Cef_Int_Regular(params);
		opt.lifetime 		= CefC_Default_LifetimeSec * 1000;
		params.chunk_num			= 0;
		params.chunk_num_f			= 1;
	}
	
	if (from_pub_f) {
		params.org.from_pub_f			= CefC_T_FROM_PUB;
	}
	
	gettimeofday (&t, NULL);
	now_time = cef_client_covert_timeval_to_us (t);
	if (nsg_flag) {
		dif_time = (uint64_t)((double) opt.lifetime * 0.8) * 1000;
		nxt_time = 0;
		end_time = now_time + 10000000;
	} else {
		dif_time = (uint64_t)((double) opt.lifetime * 0.3) * 1000;
		nxt_time = now_time + dif_time;
	}
	
	/*---------------------------------------------------------------------------
		Sends first Interest(s)
	-----------------------------------------------------------------------------*/
	app_running_f = 1;
	printf ("[cefgetstream] URI=%s\n", uri);
	if (nsg_flag) {
		cef_client_interest_input (fhdl, &opt, &params);
		printf ("[cefgetstream] Start sending Long Life Interests\n");
	} else {
		printf ("[cefgetstream] Start sending Interests\n");
		
		/* Sends Initerest(s) 		*/
		for (i = 0 ; i < pipeline ; i++) {
			cef_client_interest_input (fhdl, &opt, &params);
			params.chunk_num++;
			
			usleep (100000);
		}
		
		/* Creates the rx window 	*/
		rxwnd = (Ceft_RxWnd*) malloc (sizeof (Ceft_RxWnd));
		memset (rxwnd, 0, sizeof (Ceft_RxWnd));
		rxwnd->next = rxwnd;
		rxwnd->seq 	= 0;
		rxwnd_prev = rxwnd;
		rxwnd_head = rxwnd;
		rxwnd_tail = rxwnd;
		
		for (i = 1 ; i < pipeline ; i++) {
			rxwnd = (Ceft_RxWnd*) malloc (sizeof (Ceft_RxWnd));
			memset (rxwnd, 0, sizeof (Ceft_RxWnd));
			rxwnd->seq 	= (uint32_t) i;
			rxwnd_prev->next = rxwnd;
			rxwnd_tail = rxwnd;
			rxwnd_prev = rxwnd;
		}
		end_t.tv_sec = t.tv_sec;
	}
	
	/*---------------------------------------------------------------------------
		Main loop
	-----------------------------------------------------------------------------*/
	/* 起動表示が stdout バッファに残っているので、fd1 が stderr を指している
	   今のうちに吐き出す。その後 stdout(fd1) を本来の映像出力に戻す。 */
	fflush (stdout);
	dup2(backup_fd, 1);
	while (app_running_f) {
		if (SIG_ERR == signal (SIGINT, sigcatch)) {
			break;
		}
		
		/* Obtains UNIX time 			*/
		gettimeofday (&t, NULL);
		now_time = cef_client_covert_timeval_to_us (t);
		
		if (nsg_flag) {
			if (now_time > end_time) {
				break;
			}
		}
		
		/* Reads the message from cefnetd			*/
		res = cef_client_read (fhdl, &buff[index], CefC_AppBuff_Size - index);
		
		if (res > 0) {

			res += index;
			
			/* Updates the jitter 		*/
			if (stat_recv_frames < 1) {
				start_t.tv_sec  = t.tv_sec;
				start_t.tv_usec = t.tv_usec;
			} else {
				val = (t.tv_sec - end_t.tv_sec) * 1000000llu + (t.tv_usec - end_t.tv_usec);
				
				stat_jitter_sum    += val;
				stat_jitter_sq_sum += val * val;
				
				if (val > stat_jitter_max) {
					stat_jitter_max = val;
				}
			}
			end_t.tv_sec  = t.tv_sec;
			end_t.tv_usec = t.tv_usec;
			
			if (nsg_flag) {
				end_time = now_time + 1000000;
			}
			
			/* Incomming message process 		*/
			do {
				res = cef_client_payload_get_with_info (buff, res, &app_frame);
				
				if (app_frame.version == CefC_App_Version) {

					/* InterestReturn */
					if ( (uint8_t)app_frame.type == CefC_PT_INTRETURN ) {
						fprintf (stderr, "[cefgetstream] Incomplete\n");
						fprintf (stderr, "[cefgetstream] "
								"Received Interest Return(Type:%02x)\n", app_frame.returncode);
						app_running_f = 0;
						goto IR_RCV;
					}

					
					if (nsg_flag) {
						stat_recv_frames++;
						stat_recv_bytes += app_frame.payload_len;

						if (app_frame.chunk_num_f) {
							/* ラストホップ欠損検知: 番号の飛びを repair_table に積む
							   （再要求はメインループ末尾で行う）。
							   loss_res には「このチャンクは再要求で取り戻したのか」
							   「いつ欠損に気づいたか」「何回注文したか」が返る。
							   欠損リストの行は消える前にしか読めないのでここで取る。 */
							repair_stats.recv_chunks++;
							loss_detect_on_chunk (&detector, &repair_table,
								app_frame.chunk_num, now_time, &repair_stats, &loss_res);

							/* 先頭保護の範囲を、最初のチャンクを受けた時点で確定する。
							   最先頭の到着順入れ替わり（chunk1 が先着）でも、
							   本物の先頭は範囲の内側に入る。 */
							if (head_protect > 0 && head_limit == 0 &&
								detector.first_received_f) {
								head_limit = detector.max_seq_seen + head_protect;
								reorder.head_limit  = head_limit;
								reorder.head_margin = CefC_Reorder_Head_Margin;
							}

							/* トレース用メタ情報を組み立てて、チャンクに随伴させる。
							   出力は整列バッファを通ってから行われるため、到着時刻は
							   ここで捕まえておかないと失われる。 */
							chunk_meta.arrive_time = now_time;
							chunk_meta.detect_time = loss_res.detect_time;
							chunk_meta.n_req       = loss_res.n_req;
							chunk_meta.kind        = loss_res.repaired_f
								? CefC_Chunk_Kind_Repaired : CefC_Chunk_Kind_Normal;

							/* 先に窓を空けてから格納する（遠い未来の番号の取りこぼし
							   防止）。その後 in-order に出せる分を出力する。 */
							drain_reorder (&reorder, detector.max_seq_seen,
								giveup_margin, blk_mode_val, &first_out_f, now_time);
							reorder_store (&reorder, app_frame.chunk_num,
								app_frame.payload, app_frame.payload_len, &chunk_meta);
							drain_reorder (&reorder, detector.max_seq_seen,
								giveup_margin, blk_mode_val, &first_out_f, now_time);
						} else {
							/* チャンク番号を持たないデータは整列できないので即出力 */
							if ( blk_mode_val == 1 ) {	//NONBLOCK
								int val;
								if (!first_out_f) {
									first_out_f = 1;
									if ((val = fcntl(1, F_GETFL, 0)) >= 0) {
										fcntl(1, F_SETFL, val | O_NONBLOCK);
									}
								}
								write (1, app_frame.payload, app_frame.payload_len);
							} else {	//BLOCK
								fwrite (app_frame.payload,
									sizeof (unsigned char), app_frame.payload_len, stdout);
							}
						}
					} else {
						
						/* Inserts the received frame to the buffer 	*/
						if ((app_frame.chunk_num < rxwnd_head->seq) || 
							(app_frame.chunk_num > rxwnd_tail->seq)) {
							continue;
						}
						
						diff_seq = app_frame.chunk_num - rxwnd_head->seq;
						rxwnd = rxwnd_head;
						
						for (i = 0 ; i < diff_seq ; i++) {
							rxwnd = rxwnd->next;
						}
						
						if (rxwnd->flag != 1) {
							memcpy (
								rxwnd->buff, app_frame.payload, app_frame.payload_len);
							rxwnd->frame_size = app_frame.payload_len;
							rxwnd->flag = 1;
						}
						
						rxwnd = rxwnd_head;
						
						for (i = 0 ; i < pipeline; i++) {
							
							if (rxwnd->flag == 0) {
								params.chunk_num = rxwnd->seq;
								cef_client_interest_input (fhdl, &opt, &params);
								break;
							}
							stat_recv_frames++;
							stat_recv_bytes += app_frame.payload_len;
							
							fwrite (rxwnd->buff, 
								sizeof (unsigned char), rxwnd->frame_size, stdout);
							
							if (rxwnd->seq == UINT32_MAX) {
								fprintf (stderr, "[cefgetstream] Received the specified number of chunk\n");
								app_running_f = 0;
							}
							
							/* Updates head and tail pointers		*/
							rxwnd_head->seq 			= rxwnd_tail->seq + 1;
							rxwnd_head->flag 			= 0;
							rxwnd_head->frame_size 		= 0;
							
							rxwnd_tail->next = rxwnd_head;
							rxwnd_tail = rxwnd_head;
							
							rxwnd_head = rxwnd_tail->next;
							rxwnd_tail->next 	= NULL;
							
							rxwnd = rxwnd_head;
							
							/* Sends an interest with the next chunk number 	*/
							params.chunk_num = rxwnd_tail->seq;
							if (params.chunk_num <= UINT32_MAX) {
								cef_client_interest_input (fhdl, &opt, &params);
							}
						}
					}
				} else {
					break;
				}
			} while (res > 0);
			
			if (res > 0) {
				index = res;
			} else {
				index = 0;
			}
		}
		
		/* 欠損リストを見て、欠損チャンクを Regular Interest で再要求する
		   （Symbolicモードのみ）。どの番号を送るかの判断は repair_sched に委譲し、
		   ここは返ってきた番号を実際に cefnetd へ送る役だけを担う。 */
		if (nsg_flag) {
			repair_sched_run (&repair_table, detector.max_seq_seen,
				now_time, giveup_margin, head_limit, CefC_Reorder_Head_Margin,
				&repair_stats, &send_list);
			for (i = 0 ; i < send_list.count ; i++) {
				params_reg.chunk_num = send_list.chunks[i];
				opt.lifetime = CefC_Default_LifetimeSec * 1000;	/* 修復は通常寿命 */
				cef_client_interest_input (fhdl, &opt, &params_reg);
			}
			/* 次の Symbolic 送出のため寿命をストリーミング用に戻す */
			opt.lifetime = sg_lifetime * 1000;
		}

		/* Sends Interest with Symbolic flag to CEFORE 		*/
		if (nsg_flag) {
			if (now_time > nxt_time) {
				cef_client_interest_input (fhdl, &opt, &params);
				fprintf (stderr, "[cefgetstream] Send Long Life Interest\n");
				nxt_time = now_time + dif_time;
			}
		} else {
			if (t.tv_sec - end_t.tv_sec > 2) {
				break;
			}
		}
IR_RCV:;
	}
	
	if (nsg_flag) {
		/* 整列バッファに残った分を吐き出す（末尾に残った欠損は飛ばす＝
		   give_up_margin=0 で全ての穴を飛ばし、埋まっている分は順に出力）。 */
		gettimeofday (&t, NULL);
		now_time = cef_client_covert_timeval_to_us (t);
		drain_reorder (&reorder, detector.max_seq_seen, 0,
			blk_mode_val, &first_out_f, now_time);

		opt.lifetime = 0;
		cef_client_interest_input (fhdl, &opt, &params);

		/* 修復＋整列の成果を表示する（media を汚さないよう stderr へ） */
		fprintf (stderr, "[cefgetstream] ===== Repair Statistics =====\n");
		fprintf (stderr, "[cefgetstream] Losses detected    = "FMTU64"\n", repair_stats.loss_detected);
		fprintf (stderr, "[cefgetstream] Repaired (arrived) = "FMTU64"\n", repair_stats.repaired);
		fprintf (stderr, "[cefgetstream] Regular Interests  = "FMTU64"\n", repair_stats.regular_sent);
		fprintf (stderr, "[cefgetstream] Gave up            = "FMTU64"\n", repair_stats.gaveup);
		fprintf (stderr, "[cefgetstream] Output (in-order)  = "FMTU64"\n", reorder.out_chunks);
		fprintf (stderr, "[cefgetstream] Output bytes       = "FMTU64"\n", reorder.out_bytes);
		fprintf (stderr, "[cefgetstream] Skipped (unrecovered) = "FMTU64"\n", reorder.skipped);
		fprintf (stderr, "[cefgetstream] Give-up margin      = %u chunks\n", giveup_margin);
		fprintf (stderr, "[cefgetstream] Head protect       = %u chunks (margin %d, limit %u)\n",
			head_protect, CefC_Reorder_Head_Margin, head_limit);
		fprintf (stderr, "[cefgetstream] Head zero-filled   = "FMTU64"\n", reorder.head_skipped);

		if (g_trace_fp != NULL) {
			fprintf (stderr, "[cefgetstream] Trace lines        = "FMTU64"\n", g_trace_lines);
			fclose (g_trace_fp);
			g_trace_fp = NULL;
		}

		reorder_destroy (&reorder);
	}

	post_process (stderr);
	
	exit (0);
}

static void
print_usage (
	FILE* ofp
) {
	
	fprintf (ofp, "\nUsage: cefgetstream\n\n");
	fprintf (ofp, "  cefgetstream uri [-o] [-m chunks] [-s pipeline] [-v valid_algo] [-d config_file_dir] [-p port_num] [-z Lifetime] [-l block_mode] [--trace path] [--giveup-margin N] [--head-protect N]\n\n");
	fprintf (ofp, "  uri              Specify the URI.\n");
	fprintf (ofp, "  -o               Specify this option if content must be retrieved directly from content owner and not from intermediate cache\n");
	fprintf (ofp, "  chunks           Specify the number of chunk that you want to obtain\n");
	fprintf (ofp, "  pipeline         Number of pipeline\n");
	fprintf (ofp, "  valid_algo       Specify the validation algorithm (" CefC_ValidTypeStr_CRC32C " or " CefC_ValidTypeStr_RSA256 ")\n");
	fprintf (ofp, "  config_file_dir  Configure file directory\n");
	fprintf (ofp, "  port_num         Port Number\n");
	fprintf (ofp, "  Lifetime         Send Long Life Intereset Lifetime\n");
	fprintf (ofp, "  block_mode       0:BLOCK    1:NONBLOCK\n");
	fprintf (ofp, "  --trace path     Write a per-chunk CSV trace to 'path' (Symbolic mode only).\n");
	fprintf (ofp, "                   Columns: seq,t_arrive_us,t_detect_us,t_out_us,n_req,kind,len\n");
	fprintf (ofp, "                   Times are microseconds relative to the run start.\n");
	fprintf (ofp, "  --giveup-margin N  Chunks to wait before giving up on a lost chunk\n");
	fprintf (ofp, "                   (default %d, must be > %d and < %d).\n",
		CefC_Repair_GiveUp_Margin, CefC_Reorder_Start_Grace, CefC_Reorder_Window);
	fprintf (ofp, "  --head-protect N Protect the first N chunks (e.g. mp4 ftyp/moov index):\n");
	fprintf (ofp, "                   wait up to %d chunks and never stop re-requesting them.\n",
		CefC_Reorder_Head_Margin);
	fprintf (ofp, "                   (default %d, 0 disables).\n\n", CefC_Reorder_Head_Protect);
}

static void
post_process (
	FILE* ofp
) {
	uint64_t diff_t;
	double diff_t_dbl = 0.0;
	double thrpt = 0.0;
	uint64_t recv_bits;
	uint64_t jitter_ave;
	struct timeval diff_tval;
	int	invalid_end = 0;
	
	if (stat_recv_frames) {
		if ( !timercmp( &start_t, &end_t, != ) == 0 ) {
			if ( timercmp( &start_t, &end_t, < ) == 0 ) {
				// Invalid end time
				fprintf (ofp, "[cefgetstream] Invalid end time. No time statistics reported.\n");
				diff_t = 0;
				invalid_end = 1;
			} else {
				timersub( &end_t, &start_t, &diff_tval );
				diff_t = diff_tval.tv_sec * 1000000llu + diff_tval.tv_usec;
			}
		} else {
			//Same Time
			diff_t = 0;
		}
	} else {
		diff_t = 0;
	}
	usleep (1000000);
	fprintf (ofp, "[cefgetstream] Unconnect to cefnetd ... ");
	cef_client_close (fhdl);
	fprintf (ofp, "OK\n");
	
	fprintf (ofp, "[cefgetstream] Terminate\n");
	fprintf (ofp, "[cefgetstream] Rx Frames = "FMTU64"\n", stat_recv_frames);
	fprintf (ofp, "[cefgetstream] Rx Bytes  = "FMTU64"\n", stat_recv_bytes);
	if (diff_t > 0) {
		diff_t_dbl = (double)diff_t / 1000000.0;
		fprintf (ofp, "[cefgetstream] Duration  = %.3f sec\n", diff_t_dbl + 0.0009);
		recv_bits = stat_recv_bytes * 8;
		thrpt = (double)(recv_bits) / diff_t_dbl;
		fprintf (ofp, "[cefgetstream] Throughput = %d bps\n", (int)thrpt);
	} else {
		fprintf (ofp, "[cefgetstream] Duration  = 0.000 sec\n");
	}
	if ((stat_recv_frames > 0) && (invalid_end == 0)) {
		jitter_ave = stat_jitter_sum / stat_recv_frames;

		fprintf (ofp, "[cefgetstream] Jitter (Ave) = "FMTU64" us\n", jitter_ave);
		fprintf (ofp, "[cefgetstream] Jitter (Max) = "FMTU64" us\n", stat_jitter_max);
		fprintf (ofp, "[cefgetstream] Jitter (Var) = "FMTU64" us\n"
			, (stat_jitter_sq_sum / stat_recv_frames) - (jitter_ave * jitter_ave));
	}
}
static void
sigcatch (
	int sig
) {
	if (sig == SIGINT) {
		fprintf (stderr, "[cefgetstream] Catch the signal\n");
		app_running_f = 0;
	}
}

/*
 * 整列バッファから in-order に出せるチャンクを取り出し stdout へ書き出す。
 *   並べ替えの判断は reorder_next（cefore非依存）に任せ、ここは I/O だけ担う。
 *   block/nonblock の切替は元の Symbolic 受信と同じ挙動を保つ。
 */
static void
drain_reorder (
	CefT_Reorder_Buf*	rb,
	uint32_t			max_seq_seen,
	uint32_t			give_up_margin,
	int					blk_mode_val,
	int*				first_out_f,
	uint64_t			now_time
) {
	const unsigned char* payload;
	int len;
	uint32_t seq;
	CefT_Chunk_Meta meta;

	while (reorder_next (rb, max_seq_seen, give_up_margin,
			&payload, &len, &seq, &meta)) {
		if ( blk_mode_val == 1 ) {	//NONBLOCK
			int val;
			if (!*first_out_f) {
				*first_out_f = 1;
				if ((val = fcntl (1, F_GETFL, 0)) >= 0) {
					fcntl (1, F_SETFL, val | O_NONBLOCK);
				}
			}
			write (1, payload, len);
		} else {	//BLOCK
			fwrite (payload, sizeof (unsigned char), len, stdout);
		}
		/* 出力した瞬間を記録する。ここが「再生に間に合ったか」の基準時刻。 */
		trace_write (seq, &meta, now_time, len);
	}
}

/*
 * トレースに1行書く。--trace が指定されていなければ何もしない。
 *   時刻は g_trace_t0 からの相対 us。値が無い欄は空にする
 *   （ゼロ埋めチャンクには到着時刻が無い、など）。
 */
static void
trace_write (
	uint32_t				seq,
	const CefT_Chunk_Meta*	meta,
	uint64_t				t_out,
	int						len
) {
	const char* kind_str;

	if (g_trace_fp == NULL) {
		return;
	}

	switch (meta->kind) {
	case CefC_Chunk_Kind_Repaired:	kind_str = "repaired";	break;
	case CefC_Chunk_Kind_ZeroFill:	kind_str = "zerofill";	break;
	default:						kind_str = "normal";	break;
	}

	fprintf (g_trace_fp, "%u,", seq);

	if (meta->arrive_time) {
		fprintf (g_trace_fp, FMTU64, meta->arrive_time - g_trace_t0);
	}
	fputc (',', g_trace_fp);

	if (meta->detect_time) {
		fprintf (g_trace_fp, FMTU64, meta->detect_time - g_trace_t0);
	}
	fputc (',', g_trace_fp);

	fprintf (g_trace_fp, FMTU64",%d,%s,%d\n",
		t_out - g_trace_t0, meta->n_req, kind_str, len);

	g_trace_lines++;
}
