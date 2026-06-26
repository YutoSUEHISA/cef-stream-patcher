/*
 * reorder_buf.c
 *
 *  整列バッファ（reorder buffer）。詳細は reorder_buf.h を参照。
 *  cefore には依存しない（include しているのは標準ヘッダのみ）。
 */
#include "reorder_buf.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* 永久欠損を飛ばすとき、省略せず同じ長さのゼロを出力するための定数バッファ。
   静的なので最初から全ゼロ。reorder_next がここへのポインタを返す。 */
static const unsigned char g_reorder_zero[CefC_Reorder_Max_Payload] = {0};

int
reorder_init (
	CefT_Reorder_Buf* rb
) {
	memset (rb, 0, sizeof (*rb));
	rb->slots = (CefT_Reorder_Slot*)
		calloc (CefC_Reorder_Window, sizeof (CefT_Reorder_Slot));
	if (rb->slots == NULL) {
		return (-1);
	}
	return (0);
}

void
reorder_destroy (
	CefT_Reorder_Buf* rb
) {
	if (rb->slots != NULL) {
		free (rb->slots);
		rb->slots = NULL;
	}
}

void
reorder_store (
	CefT_Reorder_Buf*		rb,
	uint32_t				seq,
	const unsigned char*	payload,
	int						len
) {
	CefT_Reorder_Slot* s;

	/* 最初の格納で出力の起点を決める（ストリーム途中から参加するため、
	   これより前の番号は欠損ではなく「最初から要求していない」だけ）。 */
	if (!rb->started) {
		rb->started      = 1;
		rb->next_out_seq = seq;
	}

	/* 既に出力済み/スキップ済みの古い番号（再要求の重複到着など）は捨てる。 */
	if (seq < rb->next_out_seq) {
		return;
	}

	/* 窓からあふれる遠い未来の番号は今は入れられない。呼び出し側が先に
	   reorder_next で窓を空ける（古い欠損を飛ばして前進する）想定。 */
	if (seq >= rb->next_out_seq + CefC_Reorder_Window) {
		return;
	}

	/* ペイロードが想定最大を超える場合は切り詰める（安全策。通常は起きない）。 */
	if (len > CefC_Reorder_Max_Payload) {
		len = CefC_Reorder_Max_Payload;
	}

	/* チャンク長を学習する。最終チャンク以外は block_size 一定なので、
	   観測した最大長が block_size になる。永久欠損のゼロ埋めに使う。 */
	if ((uint32_t) len > rb->chunk_len) {
		rb->chunk_len = (uint32_t) len;
	}

	s = &rb->slots[seq % CefC_Reorder_Window];

	/* 同じ番号が既に入っていれば二重格納しない。 */
	if (s->used && s->seq == seq) {
		return;
	}

	s->used = 1;
	s->seq  = seq;
	s->len  = len;
	memcpy (s->payload, payload, len);
}

int
reorder_next (
	CefT_Reorder_Buf*		rb,
	uint32_t				max_seq_seen,
	uint32_t				give_up_margin,
	const unsigned char**	out_payload,
	int*					out_len
) {
	if (!rb->started) {
		return (0);
	}

	for (;;) {
		CefT_Reorder_Slot* s = &rb->slots[rb->next_out_seq % CefC_Reorder_Window];

		/* 先頭(next_out_seq)が埋まっている → in-order に出力できる。 */
		if (s->used && s->seq == rb->next_out_seq) {
			*out_payload = s->payload;
			*out_len     = s->len;
			s->used      = 0;
			rb->next_out_seq++;
			rb->out_chunks++;
			rb->out_bytes += (uint64_t) (*out_len);
			return (1);
		}

		/* 先頭が欠損中。諦め境界より遅れていれば永久欠損とみなす。
		   省略せず「学習したチャンク長ぶんのゼロ」を出力して前進する。
		   こうすると後続のバイト位置がズレず、mp4 等の索引が壊れない。 */
		if (max_seq_seen > rb->next_out_seq &&
			(max_seq_seen - rb->next_out_seq) > give_up_margin) {
			*out_payload = g_reorder_zero;
			*out_len     = (int) rb->chunk_len;	/* 通常は block_size(=1024) */
			rb->skipped++;
			rb->next_out_seq++;
			return (1);
		}

		/* まだ境界内 → 再要求で届くのを待つ。今は出力しない。 */
		return (0);
	}
}
