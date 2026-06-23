/*
 * loss_detect.c
 *
 *  番号の見張り（メモ①）＝欠損検出ロジック。詳細は loss_detect.h を参照。
 *  cefore には依存しない。進捗の表示（printf）だけは行う。
 */
#include "loss_detect.h"

#include <stdio.h>

/* エラーメッセージを画面に出すための便利な書き方（cefgetstream.c と同じ体裁） */
#define printerr(...)			fprintf(stderr,"[cefgetstream] ERROR: " __VA_ARGS__)

void
loss_detect_init (
	CefT_Loss_Detector* det
) {
	det->first_received_f = 0;
	det->max_seq_seen     = 0;
}

void
loss_detect_on_chunk (
	CefT_Loss_Detector*	det,
	CefT_Repair_Table*	tbl,
	uint32_t			seq,
	CefT_Repair_Stats*	stats
) {
	/* --- A-1: いちばん最初に受け取ったチャンクの場合 --- */
	/* ストリームの途中から要求を始めたので、これより前の番号は「欠損」では
	   ない。ここを起点にするだけ。 */
	if (!det->first_received_f) {
		det->first_received_f = 1;
		det->max_seq_seen     = seq;
		return;
	}

	/* --- A-2: 欠損リストに載っていた番号が今届いた場合 → 修復成功 --- */
	if (repair_table_find (tbl, seq) >= 0) {
		repair_table_remove (tbl, seq);
		stats->repaired++;
		fprintf (stderr, "[cefgetstream] Repaired chunk=%u\n", seq);
	}

	/* --- A-3: 番号が飛んでいた場合（欠損の検出） --- */
	/*         例: これまで最大5なのに8が来た → 6,7が欠損 */
	if (seq > det->max_seq_seen + 1) {
		uint32_t k;
		for (k = det->max_seq_seen + 1 ; k < seq ; k++) {
			if (repair_table_add (tbl, k) < 0) {
				printerr ("repair table is full (chunk=%u dropped)\n", k);
			}
			stats->loss_detected++;
			fprintf (stderr, "[cefgetstream] Detected loss chunk=%u\n", k);
		}
	}

	/* --- A-4: これまでの最大番号を更新する（メモ①） --- */
	if (seq > det->max_seq_seen) {
		det->max_seq_seen = seq;
	}
}
