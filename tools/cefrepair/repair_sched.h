/*
 * repair_sched.h
 *
 *  修復スケジューラ（設計書セクション4-4 の場面B/C/D）。
 *
 *  ■責務
 *      欠損リストを1巡し、各行について
 *        ・古すぎる/再送上限超え → 諦めて削除（場面D）
 *        ・未注文                → 初回注文すべき（場面B）
 *        ・注文済みだが返事が遅い → 再注文すべき（場面C）
 *      を判定する。「今送るべきチャンク番号」を箱に積んで返すだけで、
 *      実際の送信（cefore I/O）は呼び出し側（main）が行う。
 *      これにより、このモジュールは「時刻を入れると決定が出る」純粋関数になり、
 *      cefnetd を起動せずに単体テストできる。
 */
#ifndef __CEF_REPAIR_SCHED_H__
#define __CEF_REPAIR_SCHED_H__

#include <stdint.h>
#include "repair_table.h"

/* Regular Interest を送ってから「返事が来ない」と判断するまでの待ち時間(us)。
   100000us = 100ms。 */
#define CefC_Repair_Timeout_us		100000

/* 1つの欠損チャンクを最大何回まで注文し直すか（無限ループ防止）。 */
#define CefC_Repair_Max_Retry		3

/* 「これより古い番号はもう諦める」境界の余裕（チャンク数）。
   最新番号からこの数だけ過去までは取り寄せを試みる。cefnetd.conf の
   SYMBOLIC_BACKBUFFER（既定100）より少し小さい値にしておくのが安全。 */
#define CefC_Repair_GiveUp_Margin	80

/*
 * スケジューラが「今送るべきチャンク番号」を入れて返す箱。
 *   実際の送信は呼び出し側が chunks[0..count-1] を順に処理して行う。
 */
typedef struct {
	uint32_t	chunks[CefC_Repair_Table_Size];
	int			count;
} CefT_Repair_SendList;

/*
 * 欠損リストを1巡して、送るべき番号を out に積んで返す。
 *   tbl（last_req_time / retry_count / used）と stats を更新する。
 *   max_seq_seen は諦め判定（場面D）に使う。
 *   cefore には一切依存しない。
 */
void repair_sched_run (
	CefT_Repair_Table*		tbl,
	uint32_t				max_seq_seen,
	uint64_t				now_time,
	CefT_Repair_Stats*		stats,
	CefT_Repair_SendList*	out);

#endif // __CEF_REPAIR_SCHED_H__
