# 湖表位置删除与等值删除的序列适用域

本项目保存虚构算力设施的仓库源表、湖文件与Python读取工具。现有工具仅加载输入并显示记录数，不执行事实加工、目录提交、汇总或联接。所有设备、组织和金额均为合成。

## 字段与单位

数据文件顺序即0基物理行位置。file_id绑定路径和SHA256，data_sequence与delete_sequence仅按明确的严格小于关系匹配；快照列出各自可见文件和删除文件id。

material-index.json记录全部输入内容哈希；fixtures保存原始资料与配置，计算输出可放.workspace-output，原始资料不应被覆盖。

## 运行已有读取器

Python 3.11以上，标准库即可，不需安装第三方依赖。

    python -m src.delete_reader fixtures/manifest.json
    python -m compileall -q src tests
    python -m unittest discover -s tests -v

## 删除适用性读取层（src/delete_layer.py）

`delete_layer.py` 在不改动原始资料的前提下，按数据序列与稳定文件身份解释删除，
删除不是对主体键的永久封禁：

* **位置删除**：`target_file_id` + 从 0 开始的物理行号，只作用于登记的那个不可变
  文件；合并或重写产生新 `file_id` 后，旧位置删除不会按同一行号套用到新文件。
  负行号、重复行号、越界行号或未知目标文件都阻止读取。
* **等值删除**：保存键字段 id、键元组与 `delete_sequence`，仅在
  `data_sequence < delete_sequence` 时命中；同序列并发数据保留。键按类型与
  null-safe 语义比较，NULL 匹配 NULL（不产生 SQL 普通等号的 UNKNOWN），整数 1
  与字符串 "1" 不相等。
* **快照可见性**：只索引所选快照 `deletes` 清单中的删除文件，不扫描目录中的其他
  删除文件；清单引用未登记 id 直接报错。
* **哈希契约**：数据文件按 manifest 登记的 SHA256 核验，物理空行破坏行号契约，
  任何不符都抛 `IntegrityError`，不静默忽略。

扫描输出每条保留/删除行的原文件位置（file_id、data_sequence、path、0 基行号）与
命中删除依据；同一行命中多次只删除一次，命中依据全部保留并按删除 id 去重。

    python -I src/delete_layer.py fixtures/manifest.json --snapshot after scan

重写先对输入行应用本快照删除，再以新 `file_id` 与严格递增的新 `data_sequence`
输出到独立目录（暂存目录 + 原子替换，绝不覆盖原始资料）。新清单移除被替代数据与
只对旧文件有效的位置删除；等值删除仅在仍影响其他未重写文件时保留。老快照与老文件
字节原样保留，老行位置继续可解释，不原地重排行。

    python -I src/delete_layer.py fixtures/manifest.json rewrite --output .workspace-output/demo
    python -I src/delete_layer.py .workspace-output/demo/manifest.json --snapshot after-rewritten scan

删除索引对等值键按类型标签分桶（`--bucket-count`，桶内仍精确比较，哈希碰撞安全），
条目总数受 `--memory-budget` 约束，超限抛 `BudgetExceeded`。不存在“删除从未出现过
的键”的永久拦截规则：序列不满足严格小于关系的数据一律保留。

## 固定容器

    docker build -t sequenced-row-deletes:local .
    docker run --rm sequenced-row-deletes:local

容器镜像以完整digest固定，默认仅检查输入契约；实现新增分析功能所需依赖由开发者明确登记。
