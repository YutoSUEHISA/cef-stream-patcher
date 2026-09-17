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

/* 見張り状態を初期化する（最初に1回呼ぶ）。 */
void loss_detect_init (CefT_Loss_Detector* det);

/*
 * 受信した1チャンクの番号 seq を処理する。
 *   det（見張り状態）・tbl（欠損リスト）・stats（統計）を更新する。
 *   cefore には一切依存しない。
 */
void loss_detect_on_chunk (
	CefT_Loss_Detector*	det,
	CefT_Repair_Table*	tbl,
	uint32_t			seq,
	CefT_Repair_Stats*	stats);

#endif // __CEF_LOSS_DETECT_H__
