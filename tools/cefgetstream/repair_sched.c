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
	uint32_t				give_up_margin,
	uint32_t				head_limit,
	uint32_t				head_margin,
	uint32_t				repair_timeout_us,
	CefT_Repair_Stats*		stats,
	CefT_Repair_SendList*	out
) {
	int i;

	out->count = 0;

	for (i = 0 ; i < CefC_Repair_Table_Size ; i++) {
		CefT_Repair_Entry* e = &tbl->entries[i];
		int			in_head;
		uint32_t	margin;

		/* 空き行は飛ばす */
		if (!e->used) {
			continue;
		}

		/* 先頭保護: 先頭区間（索引などの構造情報）の欠損は、諦め境界を
		   head_margin まで延ばし、再要求の回数上限も外して待ち続ける。 */
		in_head = (head_limit > 0 && e->chunk_num < head_limit);
		margin  = in_head ? head_margin : give_up_margin;

		/* 【場面D】 古すぎる番号はもう間に合わないので諦める。
		   最新番号から margin より過去なら削除。 */
		if (max_seq_seen > margin &&
			e->chunk_num < max_seq_seen - margin) {
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
		if (now_time - e->last_req_time > repair_timeout_us) {
			if (e->retry_count >= CefC_Repair_Max_Retry && !in_head) {
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
