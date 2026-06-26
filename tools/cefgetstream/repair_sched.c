/*
 * repair_sched.c
 *
 *  修復スケジューラ（場面B/C/D）。詳細は repair_sched.h を参照。
 *  cefore には依存しない。進捗の表示（printf）だけは行う。
 */
#include "repair_sched.h"

#include <stdio.h>

/* 送るべき番号を out に1件積む（あふれたら捨てる）。 */
static void
send_push (
	CefT_Repair_SendList*	out,
	uint32_t				chunk_num
) {
	if (out->count < CefC_Repair_Table_Size) {
		out->chunks[out->count++] = chunk_num;
	}
}

void
repair_sched_run (
	CefT_Repair_Table*		tbl,
	uint32_t				max_seq_seen,
	uint64_t				now_time,
	CefT_Repair_Stats*		stats,
	CefT_Repair_SendList*	out
) {
	int i;

	out->count = 0;

	for (i = 0 ; i < CefC_Repair_Table_Size ; i++) {
		CefT_Repair_Entry* e = &tbl->entries[i];

		/* 空き行は飛ばす */
		if (!e->used) {
			continue;
		}

		/* 【場面D】 古すぎる番号はもう間に合わないので諦める。
		   最新番号から CefC_Repair_GiveUp_Margin より過去なら削除。 */
		if (max_seq_seen > CefC_Repair_GiveUp_Margin &&
			e->chunk_num < max_seq_seen - CefC_Repair_GiveUp_Margin) {
			fprintf (stderr, "[cefgetstream] Give up chunk=%u (too old)\n", e->chunk_num);
			e->used = 0;
			stats->gaveup++;
			continue;
		}

		/* 【場面B】 まだ一度も注文していない行（retry_count==0） */
		if (e->retry_count == 0) {
			send_push (out, e->chunk_num);
			e->last_req_time = now_time;
			e->retry_count   = 1;
			stats->regular_sent++;
			fprintf (stderr, "[cefgetstream] Request(1st) chunk=%u\n", e->chunk_num);
			continue;
		}

		/* 【場面C】 注文済みだが、待ち時間を過ぎても返事が来ない行 */
		if (now_time - e->last_req_time > CefC_Repair_Timeout_us) {
			if (e->retry_count >= CefC_Repair_Max_Retry) {
				/* 上限まで注文したのに届かない → 諦めて削除 */
				fprintf (stderr, "[cefgetstream] Give up chunk=%u (max retry)\n", e->chunk_num);
				e->used = 0;
				stats->gaveup++;
			} else {
				/* もう一度注文し直し、回数を1つ増やす */
				send_push (out, e->chunk_num);
				e->last_req_time = now_time;
				e->retry_count++;
				stats->regular_sent++;
				fprintf (stderr, "[cefgetstream] Request(retry %d) chunk=%u\n",
						e->retry_count, e->chunk_num);
			}
		}
	}
}
