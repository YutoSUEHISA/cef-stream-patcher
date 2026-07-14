/*
 * test_logic.c
 *
 *  cefgetstream の「頭脳」4モジュール（cefore非依存）の単体テスト。
 *    - repair_table : 欠損リスト（追加・検索・削除・重複・満杯）
 *    - loss_detect  : 欠損検出（初回・順調・飛び・修復消し込み・最大更新）
 *    - repair_sched : 修復スケジューラ（場面B 初回要求 / C 再送 / D 諦め）
 *    - reorder_buf  : 整列（順調・並べ替え・永久欠損の飛ばし・末尾フラッシュ）
 *
 *  cefnetd も cefore も不要。標準Cコンパイラだけでビルド・実行できる:
 *      gcc -I.. -o test_logic test_logic.c \
 *          ../repair_table.c ../loss_detect.c ../repair_sched.c ../reorder_buf.c
 *      ./test_logic
 *  （tests/ ディレクトリの Makefile を使うなら `make run`）
 */
#include <stdio.h>
#include <string.h>

#include "repair_table.h"
#include "loss_detect.h"
#include "repair_sched.h"
#include "reorder_buf.h"

/*-------------------------------------------------------------------------
	ごく小さなテスト用ヘルパ（assert の代わり。失敗を数えて続行する）
-------------------------------------------------------------------------*/
static int g_fail = 0;
static int g_total = 0;

#define CHECK(cond) do {                                            \
		g_total++;                                                  \
		if (cond) {                                                 \
			printf ("  [PASS] %s\n", #cond);                        \
		} else {                                                    \
			printf ("  [FAIL] %s  (line %d)\n", #cond, __LINE__);   \
			g_fail++;                                               \
		}                                                           \
	} while (0)

/*=========================================================================
	repair_table のテスト
=========================================================================*/
static void
test_repair_table (void)
{
	CefT_Repair_Table tbl;
	int i;

	printf ("\n== repair_table ==\n");
	repair_table_init (&tbl);

	/* 空の表では何も見つからない */
	CHECK (repair_table_find (&tbl, 5) < 0);

	/* 追加すると見つかる */
	CHECK (repair_table_add (&tbl, 5) == 0);
	CHECK (repair_table_find (&tbl, 5) >= 0);

	/* 同じ番号は二重登録しない（成功扱いで 0、件数は増えない） */
	CHECK (repair_table_add (&tbl, 5) == 0);

	/* 別番号を足して、片方だけ消す */
	CHECK (repair_table_add (&tbl, 7) == 0);
	repair_table_remove (&tbl, 5);
	CHECK (repair_table_find (&tbl, 5) < 0);   /* 消えた   */
	CHECK (repair_table_find (&tbl, 7) >= 0);  /* 残ってる */

	/* 満杯テスト: 表を埋め切ると次の追加は -1 */
	repair_table_init (&tbl);
	for (i = 0 ; i < CefC_Repair_Table_Size ; i++) {
		if (repair_table_add (&tbl, (uint32_t) i) != 0) {
			break;
		}
	}
	CHECK (i == CefC_Repair_Table_Size);                 /* 全部入った */
	CHECK (repair_table_add (&tbl, 999999) == -1);       /* もう入らない */
}

/*=========================================================================
	loss_detect のテスト
=========================================================================*/
static void
test_loss_detect (void)
{
	CefT_Repair_Table  tbl;
	CefT_Loss_Detector det;
	CefT_Repair_Stats  stats;

	printf ("\n== loss_detect ==\n");
	repair_table_init (&tbl);
	loss_detect_init  (&det);
	memset (&stats, 0, sizeof (stats));

	/* 最初のチャンク(5): 起点になるだけ。欠損は出ない */
	loss_detect_on_chunk (&det, &tbl, 5, &stats);
	CHECK (det.max_seq_seen == 5);
	CHECK (stats.loss_detected == 0);

	/* 順調(6): 何も起きない */
	loss_detect_on_chunk (&det, &tbl, 6, &stats);
	CHECK (det.max_seq_seen == 6);
	CHECK (stats.loss_detected == 0);

	/* 飛び(9): 7,8 を欠損として検出 */
	loss_detect_on_chunk (&det, &tbl, 9, &stats);
	CHECK (det.max_seq_seen == 9);
	CHECK (stats.loss_detected == 2);
	CHECK (repair_table_find (&tbl, 7) >= 0);
	CHECK (repair_table_find (&tbl, 8) >= 0);

	/* 抜けていた 7 が遅れて到着 → 修復成功・リストから消える */
	loss_detect_on_chunk (&det, &tbl, 7, &stats);
	CHECK (stats.repaired == 1);
	CHECK (repair_table_find (&tbl, 7) < 0);

	/* 既に観測済みの番号の重複到着(9) は誤検出しない */
	loss_detect_on_chunk (&det, &tbl, 9, &stats);
	CHECK (stats.loss_detected == 2);   /* 増えない */
	CHECK (det.max_seq_seen == 9);
}

/*=========================================================================
	repair_sched のテスト
=========================================================================*/
static void
test_repair_sched (void)
{
	CefT_Repair_Table    tbl;
	CefT_Repair_Stats    stats;
	CefT_Repair_SendList out;
	uint64_t now;

	printf ("\n== repair_sched ==\n");

	/*--- 場面B/C: 初回要求 → タイムアウト再送 → 上限で諦め ---*/
	repair_table_init (&tbl);
	memset (&stats, 0, sizeof (stats));
	repair_table_add (&tbl, 10);     /* 欠損 10 を登録 */
	now = 1000;

	/* 1回目: 未注文なので初回要求が出る（場面B） */
	repair_sched_run (&tbl, /*max_seq*/ 10, now, &stats, &out);
	CHECK (out.count == 1);
	CHECK (out.chunks[0] == 10);
	CHECK (stats.regular_sent == 1);

	/* すぐ再実行: タイムアウト前なので何も出ない */
	repair_sched_run (&tbl, 10, now, &stats, &out);
	CHECK (out.count == 0);
	CHECK (stats.regular_sent == 1);

	/* タイムアウト経過ごとに再送（場面C）。上限 CefC_Repair_Max_Retry 回まで */
	now += CefC_Repair_Timeout_us + 1;
	repair_sched_run (&tbl, 10, now, &stats, &out);   /* retry 2回目 */
	CHECK (out.count == 1);
	CHECK (stats.regular_sent == 2);

	now += CefC_Repair_Timeout_us + 1;
	repair_sched_run (&tbl, 10, now, &stats, &out);   /* retry 3回目 */
	CHECK (out.count == 1);
	CHECK (stats.regular_sent == 3);

	/* さらに経過 → 上限到達で諦め（場面C の諦め側）。送信は増えず gaveup++ */
	now += CefC_Repair_Timeout_us + 1;
	repair_sched_run (&tbl, 10, now, &stats, &out);
	CHECK (out.count == 0);
	CHECK (stats.regular_sent == 3);
	CHECK (stats.gaveup == 1);
	CHECK (repair_table_find (&tbl, 10) < 0);   /* 表からも消えた */

	/*--- 場面D: 最新から境界より古い番号は即諦め ---*/
	repair_table_init (&tbl);
	memset (&stats, 0, sizeof (stats));
	repair_table_add (&tbl, 5);
	/* max_seq=200 なら 5 は 200-80=120 より古い → 取り寄せ前に諦める */
	repair_sched_run (&tbl, /*max_seq*/ 200, 1000, &stats, &out);
	CHECK (out.count == 0);
	CHECK (stats.gaveup == 1);
	CHECK (stats.regular_sent == 0);
	CHECK (repair_table_find (&tbl, 5) < 0);
}

/*=========================================================================
	reorder_buf のテスト
=========================================================================*/

/* テスト補助: 1バイトのペイロードでチャンクを格納する */
static void
rb_store1 (CefT_Reorder_Buf* rb, uint32_t seq, unsigned char b)
{
	reorder_store (rb, seq, &b, 1);
}

/* テスト補助: 出せる分を全部取り出し、出力された seq を順に out[] に集める。
   戻り値は取り出した個数。reorder_next が seq を返さないので out_chunks の
   増分から逆算するのではなく、ここでは「中身(1バイト)」を集めて検証する。 */
static int
rb_drain_collect (CefT_Reorder_Buf* rb, uint32_t max_seq, uint32_t margin,
                  unsigned char* collected, int cap)
{
	const unsigned char* p;
	int len;
	int n = 0;
	while (n < cap && reorder_next (rb, max_seq, margin, &p, &len)) {
		collected[n++] = p[0];
	}
	return (n);
}

static void
test_reorder_buf (void)
{
	CefT_Reorder_Buf rb;
	unsigned char got[128];
	int n;

	printf ("\n== reorder_buf ==\n");
	CHECK (reorder_init (&rb) == 0);

	/*--- 出力開始前の猶予: 最初のチャンクは即出力されず、
	      max_seq_seen が 起点+Start_Grace を超えてから出る ---*/
	rb_store1 (&rb, 10, 'A');
	n = rb_drain_collect (&rb, 10, CefC_Repair_GiveUp_Margin, got, 64);
	CHECK (n == 0);                             /* 猶予中は保留 */
	n = rb_drain_collect (&rb, 10 + CefC_Reorder_Start_Grace - 1,
	                      CefC_Repair_GiveUp_Margin, got, 64);
	CHECK (n == 0);                             /* 境界の1つ手前もまだ保留 */
	n = rb_drain_collect (&rb, 10 + CefC_Reorder_Start_Grace,
	                      CefC_Repair_GiveUp_Margin, got, 64);
	CHECK (n == 1 && got[0] == 'A');            /* 猶予明けで next_out=10 が出る */

	/*--- 順調: 出力開始後は 11,12 を順に格納して順に出る ---*/
	rb_store1 (&rb, 11, 'B');
	rb_store1 (&rb, 12, 'C');
	n = rb_drain_collect (&rb, 26, CefC_Repair_GiveUp_Margin, got, 64);
	CHECK (n == 2 && got[0] == 'B' && got[1] == 'C');
	CHECK (rb.out_chunks == 3);

	/*--- 並べ替え: 先に 14 が来て、その後 13 が来たら 13,14 の順に出る ---*/
	rb_store1 (&rb, 14, 'E');                   /* 13 がまだ無いので出ない */
	n = rb_drain_collect (&rb, 14, CefC_Repair_GiveUp_Margin, got, 64);
	CHECK (n == 0);                             /* 欠損 13 を待つ */

	rb_store1 (&rb, 13, 'D');                   /* 遅れて 13 到着 */
	n = rb_drain_collect (&rb, 14, CefC_Repair_GiveUp_Margin, got, 64);
	CHECK (n == 2 && got[0] == 'D' && got[1] == 'E');

	/*--- 永久欠損の飛ばし: 15 の後いきなり 100 が来る（16..99 は来ない） ---*/
	/* 実機と同じ手順: max を更新 → drain（窓を空ける/古い穴を飛ばす）→ store */
	reorder_destroy (&rb);
	CHECK (reorder_init (&rb) == 0);
	rb_store1 (&rb, 15, 'X');
	n = rb_drain_collect (&rb, 15 + CefC_Reorder_Start_Grace,
	                      CefC_Repair_GiveUp_Margin, got, 64);
	CHECK (n == 1 && got[0] == 'X');            /* 猶予明けで 15 出力, next_out=16 */

	/* 100 到着。drain(max=100) で境界(80)より古い穴を飛ばす:
	   16..(100-80-1)=19 を飛ばし、next_out=20 で停止。
	   飛ばす際は省略せず「学習したチャンク長(ここでは1)のゼロ」が出力される */
	{
		const unsigned char* p; int len;
		int drained  = 0;
		int zeros_ok = 1;
		while (reorder_next (&rb, 100, CefC_Repair_GiveUp_Margin, &p, &len)) {
			if (len != 1 || p[0] != 0) {
				zeros_ok = 0;
			}
			drained++;
		}
		CHECK (drained == 4);                   /* 16..19 の4穴がゼロ埋めで出る */
		CHECK (zeros_ok == 1);
	}
	CHECK (rb.skipped == 4);                    /* 16,17,18,19 を飛ばした */
	rb_store1 (&rb, 100, 'Y');                  /* 100-20=80 < 窓128 なので入る */
	n = rb_drain_collect (&rb, 100, CefC_Repair_GiveUp_Margin, got, 64);
	CHECK (n == 0);                             /* next_out=20 はまだ境界内で待ち */

	/*--- 古すぎる重複到着は捨てる: 既に出力/飛ばし済みの 15 が再来 ---*/
	rb_store1 (&rb, 15, 'Z');
	n = rb_drain_collect (&rb, 100, CefC_Repair_GiveUp_Margin, got, 64);
	CHECK (n == 0);                             /* 何も出ない（捨てられた） */

	/*--- 末尾フラッシュ: margin=0 で残りの穴(20..99)をゼロ埋めし、100 を吐き出す ---*/
	n = rb_drain_collect (&rb, 100, 0, got, 128);
	CHECK (n == 81);                            /* ゼロ埋め80個 + 'Y' の計81個 */
	CHECK (got[0] == 0 && got[79] == 0 && got[80] == 'Y');
	CHECK (rb.skipped == 84);                   /* 4 + 80 */

	/*--- out_bytes はゼロ埋め分も含む＝ファイル実サイズと一致する。
	      このテストは全ペイロード長1なので out_chunks + skipped と等しい ---*/
	CHECK (rb.out_bytes == rb.out_chunks + rb.skipped);

	reorder_destroy (&rb);
}

/*=========================================================================
	reorder_buf: ストリーム最先頭の到着順入れ替わり（t1rep 4/50 ランの実バグ）
=========================================================================*/
static void
test_reorder_start_swap (void)
{
	CefT_Reorder_Buf rb;
	unsigned char got[64];
	int n;

	printf ("\n== reorder_buf (最先頭の到着順入れ替わり) ==\n");

	/*--- chunk1 が chunk0 より先に届くケース（−1024B ランの再現） ---*/
	CHECK (reorder_init (&rb) == 0);
	rb_store1 (&rb, 1, 'B');                    /* 先着 → いったん起点=1     */
	n = rb_drain_collect (&rb, 1, CefC_Repair_GiveUp_Margin, got, 64);
	CHECK (n == 0);                             /* 猶予中: まだ出力しない     */
	rb_store1 (&rb, 0, 'A');                    /* 本物の先頭が遅れて到着 →  */
	                                            /* 捨てずに起点を 0 へ下げ直す */
	n = rb_drain_collect (&rb, CefC_Reorder_Start_Grace,
	                      CefC_Repair_GiveUp_Margin, got, 64);
	CHECK (n == 2 && got[0] == 'A' && got[1] == 'B');   /* 0,1 の順で出る    */
	CHECK (rb.skipped == 0);
	reorder_destroy (&rb);

	/*--- chunk0,1 の2個が chunk2 より遅れるケース（−2048B ランの再現） ---*/
	CHECK (reorder_init (&rb) == 0);
	rb_store1 (&rb, 2, 'C');
	rb_store1 (&rb, 0, 'A');
	rb_store1 (&rb, 1, 'B');
	n = rb_drain_collect (&rb, CefC_Reorder_Start_Grace,
	                      CefC_Repair_GiveUp_Margin, got, 64);
	CHECK (n == 3 && got[0] == 'A' && got[1] == 'B' && got[2] == 'C');
	reorder_destroy (&rb);

	/*--- 途中参加の重複捨ては維持: 猶予を超える古さは rebase しない ---*/
	CHECK (reorder_init (&rb) == 0);
	rb_store1 (&rb, 100, 'M');                  /* 途中参加: 起点=100        */
	rb_store1 (&rb, 50, 'S');                   /* 100-50=50 > 猶予 → 捨てる */
	n = rb_drain_collect (&rb, 100 + CefC_Reorder_Start_Grace,
	                      CefC_Repair_GiveUp_Margin, got, 64);
	CHECK (n == 1 && got[0] == 'M');            /* 'S' は現れない            */
	reorder_destroy (&rb);

	/*--- ストリームが猶予より短くても終端フラッシュ(margin=0)で出る ---*/
	CHECK (reorder_init (&rb) == 0);
	rb_store1 (&rb, 3, 'Q');
	n = rb_drain_collect (&rb, 3, CefC_Repair_GiveUp_Margin, got, 64);
	CHECK (n == 0);                             /* 猶予中                    */
	n = rb_drain_collect (&rb, 3, 0, got, 64);
	CHECK (n == 1 && got[0] == 'Q');            /* 終端フラッシュは待たない  */
	reorder_destroy (&rb);
}

/*=========================================================================
	main
=========================================================================*/
int
main (void)
{
	printf ("=== cefgetstream logic unit tests ===\n");
	printf ("(loss_detect / repair_sched の進捗ログは stderr に出ます)\n");

	test_repair_table ();
	test_loss_detect ();
	test_repair_sched ();
	test_reorder_buf ();
	test_reorder_start_swap ();

	printf ("\n=== Summary: %d/%d passed, %d failed ===\n",
		g_total - g_fail, g_total, g_fail);

	return (g_fail == 0 ? 0 : 1);
}
