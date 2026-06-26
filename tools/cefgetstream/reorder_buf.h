/*
 * reorder_buf.h
 *
 *  整列バッファ（reorder buffer）＝ Symbolic モードで受信したチャンクを
 *  チャンク番号順に並べ替えてから出力するためのリングバッファ。
 *
 *  ■責務
 *      到着順がばらばら／欠損後に再要求で遅れて届くチャンクを、
 *      「次に出力すべき番号(next_out_seq)」を基準に in-order へ整える。
 *        ・先頭が埋まっていれば in-order に出力できる（reorder_next が返す）
 *        ・先頭が欠損中なら出力を止めて待つ
 *        ・諦め境界(give_up_margin)より遅れた番号は永久欠損として飛ばす
 *      I/O（stdout への書き出し）は行わない。出力すべきチャンクを返すだけ。
 *      cefore には依存しない純粋ロジックなので単体テストできる。
 */
#ifndef __CEF_REORDER_BUF_H__
#define __CEF_REORDER_BUF_H__

#include <stdint.h>

/* 整列窓のスロット数。repair の諦め境界(CefC_Repair_GiveUp_Margin=80)より
   大きくすること（窓 > 境界。さもないと飛ばす前にスロットが衝突する）。 */
#define CefC_Reorder_Window			128

/* 1チャンクの最大ペイロード長（cefore の CefC_Max_Length=65535 に合わせる）。 */
#define CefC_Reorder_Max_Payload	65535

/*
 * 整列窓の1スロット。チャンク番号 seq のペイロードを1つ保持する。
 */
typedef struct {
	int				used;		/* このスロットが埋まっているか(1=使用中)     */
	uint32_t		seq;		/* 格納しているチャンク番号                  */
	int				len;		/* ペイロード長                              */
	unsigned char	payload[CefC_Reorder_Max_Payload];
} CefT_Reorder_Slot;

/*
 * 整列バッファ本体。slots は heap に確保する（窓全体で数MBになるため）。
 */
typedef struct {
	CefT_Reorder_Slot*	slots;			/* 長さ CefC_Reorder_Window の配列   */
	uint32_t			next_out_seq;	/* 次に出力すべきチャンク番号        */
	int					started;		/* 最初のチャンクで基準が定まったか  */
	uint32_t			chunk_len;		/* 学習したチャンク長(=block_size)。  */
									/* 永久欠損を飛ばす際、この長さ分の   */
									/* ゼロを出力してバイト位置を保つ。   */

	/* 統計（main が最後に表示する） */
	uint64_t			out_chunks;		/* in-order に出力したチャンク数     */
	uint64_t			out_bytes;		/* 出力したバイト数                  */
	uint64_t			skipped;		/* 永久欠損として飛ばした数          */
} CefT_Reorder_Buf;

/* 初期化（slots を確保）。成功 0 / 失敗 -1。 */
int  reorder_init    (CefT_Reorder_Buf* rb);

/* 後始末（slots を解放）。 */
void reorder_destroy (CefT_Reorder_Buf* rb);

/*
 * 受信した1チャンクを整列窓に格納する。
 *   ・最初の格納で next_out_seq を seq に合わせる（途中参加の起点）。
 *   ・既に出力/スキップ済み(seq < next_out_seq)なら捨てる。
 *   ・窓からあふれる(seq >= next_out_seq + 窓)場合は捨てる。呼び出し側が
 *     先に reorder_next で窓を空ければ（飛ばし前進）通常は収まる。
 */
void reorder_store   (CefT_Reorder_Buf*	rb,
                      uint32_t			seq,
                      const unsigned char* payload,
                      int				len);

/*
 * in-order に出力できるチャンクを1つ取り出す。while で回して使う。
 *   戻り値 1: out_payload/out_len に出力すべきデータを格納した。
 *   戻り値 0: いま出力できるものは無い（欠損待ち、または末尾まで出した）。
 *   max_seq_seen と give_up_margin により「諦め境界より遅れた欠損は飛ばす」
 *   を内部で処理する。ただし**飛ばす際は省略せず、学習したチャンク長分の
 *   ゼロ(out_payload=ゼロ列)を返す**。これで後続のバイト位置がズレず、mp4 等
 *   のバイトオフセット索引が壊れない（飛ばした数は skipped に計上）。
 *   返したポインタは次に reorder_* を呼ぶまで有効。呼び出し側は即座に書き出す。
 */
int  reorder_next    (CefT_Reorder_Buf*	rb,
                      uint32_t			max_seq_seen,
                      uint32_t			give_up_margin,
                      const unsigned char** out_payload,
                      int*				out_len);

#endif // __CEF_REORDER_BUF_H__
