# cefgetstream 頭脳ロジックの単体テスト用（autotools とは独立した手書き Makefile）。
#
#   make        … テストをビルド
#   make run    … ビルドして実行（失敗があれば終了コード非0）
#   make clean  … 生成物を削除
#
# cefore も cefnetd も不要。標準Cコンパイラ(gcc/clang)だけでビルドできる。

CC      ?= gcc
CFLAGS  ?= -Wall -Wextra -g -I..

# テスト対象の cefore非依存モジュール（親ディレクトリにある）
MODULES = ../repair_table.c ../loss_detect.c ../repair_sched.c ../reorder_buf.c

TARGET  = test_logic

$(TARGET): test_logic.c $(MODULES) ../repair_table.h ../loss_detect.h ../repair_sched.h ../reorder_buf.h
	$(CC) $(CFLAGS) -o $(TARGET) test_logic.c $(MODULES)

.PHONY: run clean
run: $(TARGET)
	./$(TARGET)

clean:
	rm -f $(TARGET)
