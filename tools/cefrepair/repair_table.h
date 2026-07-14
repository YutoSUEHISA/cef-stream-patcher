/*
 * repair_table.h
 *
 *  欠損リスト（メモ②）のデータ構造とその操作。
 *
 *  ■責務
 *      「どのチャンク番号が」「いつ注文されて」「何回注文したか」を覚える表。
 *      cefore（ICN本体）には一切依存しない純粋なデータ構造なので、
 *      cefnetd を起動しなくても単体テストできる。
 */
#ifndef __CEF_REPAIR_TABLE_H__
#define __CEF_REPAIR_TABLE_H__

#include <stdint.h>

/* 欠損リストの最大行数。これ以上の欠損は同時に管理できない。 */
#define CefC_Repair_Table_Size		4096

/*
 * 欠損リストの1行ぶんを表す箱。
 */
typedef struct {
	int			used;			/* この行が使用中か（1=使用中, 0=空き）         */
	uint32_t	chunk_num;		/* 欠損しているチャンク番号                    */
	uint64_t	last_req_time;	/* 最後に Regular Interest を送った時刻(us)     */
	int			retry_count;	/* これまでに注文した回数                      */
} CefT_Repair_Entry;

/*
 * 欠損リスト本体（配列をまとめた箱）。
 *   グローバル変数をやめてこの箱を引数で受け渡すことで、
 *   検出ロジック・スケジューラを cefore 抜きでテストできるようにする。
 */
typedef struct {
	CefT_Repair_Entry	entries[CefC_Repair_Table_Size];
} CefT_Repair_Table;

/*
 * 統計（最後に成果を表示するための数え上げ箱）。
 *   各モジュールが自分の担当ぶんを加算し、main が最後にまとめて表示する。
 */
typedef struct {
	uint64_t	recv_chunks;	/* 受信したチャンク数             */
	uint64_t	loss_detected;	/* 欠損として検出した数           */
	uint64_t	repaired;		/* 修復に成功した（届いた）数     */
	uint64_t	regular_sent;	/* 送った Regular Interest の総数 */
	uint64_t	gaveup;			/* 諦めた数                       */
} CefT_Repair_Stats;

/* 表を空にする（最初に1回呼ぶ）。 */
void repair_table_init   (CefT_Repair_Table* tbl);

/* 1件追加する。すでに同じ番号があれば二重登録しない。
   戻り値: 0=追加できた（または既に登録済み）, -1=満杯で追加できなかった。 */
int  repair_table_add    (CefT_Repair_Table* tbl, uint32_t chunk_num);

/* 指定番号の行を削除する（修復成功時に呼ぶ）。 */
void repair_table_remove (CefT_Repair_Table* tbl, uint32_t chunk_num);

/* 指定番号を探す。見つかれば行番号(0以上)、なければ -1 を返す。 */
int  repair_table_find   (CefT_Repair_Table* tbl, uint32_t chunk_num);

#endif // __CEF_REPAIR_TABLE_H__
