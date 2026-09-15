/*
 * loss_detect.h
 *
 *  番号の見張り（メモ①）＝欠損検出ロジック。
 *
 *  ■責務
 *      受信したチャンク番号を1つずつ受け取り、
 *        ・欠損リストに載っていた番号なら「修復成功」として消す
 *        ・番号が飛んでいたら、その間を「欠損」として積む
 *        ・これまでの最大番号を更新する
 *      を行う。cefore には依存しない純粋ロジックなので単体テスト可能。
 */
#ifndef __CEF_LOSS_DETECT_H__
#define __CEF_LOSS_DETECT_H__

#include <stdint.h>
#include "repair_table.h"

/*
 * 番号の見張り状態（メモ①にあたる）。
 */
typedef struct {
	int			first_received_f;	/* 最初のチャンクを受信済みか        */
	uint32_t	max_seq_seen;		/* これまでに見た最大のチャンク番号  */
} CefT_Loss_Detector;

/*
 * 1チャンクを処理した結果のうち、トレース記録に必要な情報を呼び出し側へ返す箱。
 *   「このチャンクは再要求で取り戻したものか」「いつ欠損に気づいたか」
 *   「何回注文したか」は欠損リストの行が消される瞬間にしか分からないため、
 *   loss_detect_on_chunk が消す前に写し取って返す。
 */
typedef struct {
	int			repaired_f;		/* 1=このチャンクは再要求で届いた（修復成功） */
	uint64_t	detect_time;	/* 欠損として最初に検出した時刻(us)。        */
								/* repaired_f==1 のときのみ意味を持つ。      */
	int			n_req;			/* 送った Regular Interest の回数            */
} CefT_Loss_Result;

/* 見張り状態を初期化する（最初に1回呼ぶ）。 */
void loss_detect_init (CefT_Loss_Detector* det);

/*
 * 受信した1チャンクの番号 seq を処理する。
 *   det（見張り状態）・tbl（欠損リスト）・stats（統計）を更新する。
 *   now_time は「今の時刻(us)」。新たに検出した欠損の detect_time になる。
 *   res は NULL 可（トレースが不要なら渡さなくてよい）。
 *   cefore には一切依存しない。
 */
void loss_detect_on_chunk (
	CefT_Loss_Detector*	det,
	CefT_Repair_Table*	tbl,
	uint32_t			seq,
	uint64_t			now_time,
	CefT_Repair_Stats*	stats,
	CefT_Loss_Result*	res);

#endif // __CEF_LOSS_DETECT_H__
