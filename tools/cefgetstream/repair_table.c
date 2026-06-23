/*
 * repair_table.c
 *
 *  欠損リスト（メモ②）の操作。詳細は repair_table.h を参照。
 *  cefore には依存しない（include しているのは標準ヘッダのみ）。
 */
#include "repair_table.h"

#include <string.h>

void
repair_table_init (
	CefT_Repair_Table* tbl
) {
	memset (tbl, 0, sizeof (*tbl));
}

int
repair_table_add (
	CefT_Repair_Table*	tbl,
	uint32_t			chunk_num
) {
	int i;

	/* すでに登録済みなら二重登録しない（成功扱い） */
	if (repair_table_find (tbl, chunk_num) >= 0) {
		return (0);
	}

	/* 空いている行を探して、そこに書き込む */
	for (i = 0 ; i < CefC_Repair_Table_Size ; i++) {
		if (!tbl->entries[i].used) {
			tbl->entries[i].used          = 1;
			tbl->entries[i].chunk_num     = chunk_num;
			tbl->entries[i].last_req_time = 0;
			tbl->entries[i].retry_count   = 0;	/* まだ注文していない印 */
			return (0);
		}
	}

	/* 満杯。ログ出力は呼び出し側に任せる（このモジュールは I/O しない）。 */
	return (-1);
}

void
repair_table_remove (
	CefT_Repair_Table*	tbl,
	uint32_t			chunk_num
) {
	int idx = repair_table_find (tbl, chunk_num);
	if (idx >= 0) {
		tbl->entries[idx].used = 0;	/* 使用中の旗を下ろせば「空き」になる */
	}
}

int
repair_table_find (
	CefT_Repair_Table*	tbl,
	uint32_t			chunk_num
) {
	int i;
	for (i = 0 ; i < CefC_Repair_Table_Size ; i++) {
		if (tbl->entries[i].used && tbl->entries[i].chunk_num == chunk_num) {
			return (i);
		}
	}
	return (-1);
}
