# inventory_system_refactor

<<<<<<< HEAD
这是基于原始 `V6.py` 拆分出来的**第二轮精修版**。目标不是一次性把所有业务逻辑都“重写”，而是优先做到下面三件事：

1. **把单文件拆开，降低理解成本**
2. **把依赖方向拉直，减少循环导入风险**
3. **给后续继续重构留出清晰的接手路径**

这版适合作为你后续继续维护、排查问题、逐步重构的基础项目骨架。

---

## 这次第二轮精修做了什么

相比第一轮“先拆开再说”的过渡版，这一版继续做了 4 类收敛：

### 1）移除 `views.common` 聚合依赖

第一轮为了快速把 `V6.py` 切开，保留了一个 `views/common.py` 聚合导入层。这样虽然能快速落地，但会带来两个问题：

- 页面文件依赖关系不透明
- 某个页面实际需要什么模块，很难一眼看出来

第二轮里，已经把各页面改成**显式导入**：

- `views/home.py` 只导入首页需要的 `FUNC_MAP`、`go_page`、`streamlit`
- `views/query.py` 只导入查询页需要的 `CUSTOM_MODEL_ORDER`、`PLOTLY_AVAILABLE`、`get_data` 等
- `views/log_viewer.py` 改为直接调用 `crud.logs.get_transaction_logs()`，不再在视图层直接写 SQL

现在 `views/common.py` 只保留了一段**弃用说明**，不再参与正常执行流程。

### 2）新增应用启动编排层 `core/bootstrap.py`

把原来 `app.py` 里散落的应用初始化逻辑收束到一个入口：

- 页面配置 `configure_page()`
- 全局样式 `apply_global_styles()`
- 文件目录创建 `ensure_storage_dirs()`
- Session 初始化 `init_session_state()`
- 数据库初始化 `init_mysql_tables()`
- 合同文件清理 `clean_expired_contracts()`

这样 `app.py` 不再承担太多启动细节，只负责：

- 初始化应用
- 判断是否登录
- 渲染侧边栏
- 路由到当前页面

### 3）新增路由层 `views/router.py`

将页面路由注册集中管理：

- 页面名和渲染函数的映射统一收敛到 `ROUTES`
- `app.py` 不需要手写一长串 `if/elif`
- 后续新增页面时，只需要：
  - 新建 `views/xxx.py`
  - 在 `views/router.py` 注册 `render_xxx`

### 4）修正了一处典型的“UI 层越界访问数据库”问题

`views/log_viewer.py` 原本直接访问数据库并执行 SQL。

第二轮已改成：

- 由 `crud.logs.get_transaction_logs()` 返回 DataFrame
- 视图层只负责显示数据

这是你后续继续做“UI 与逻辑分离”的参考模式。

---

## 当前目录结构

```text
inventory_system_refactor/
│
├── app.py                     # 唯一主入口：只负责初始化、登录判断、侧边栏、页面分发
├── config.py                  # 配置中心：环境变量、常量、权限、机型排序、CSS、可选依赖
├── database.py                # 数据库连接与建表逻辑
├── README.md                  # 当前说明文档
├── .env.example               # 环境变量示例
│
├── core/                      # 核心应用层（认证、权限、文件管理、OCR、启动编排）
│   ├── auth.py
│   ├── bootstrap.py
│   ├── file_manager.py
│   ├── metrics.py
│   ├── navigation.py
│   ├── ocr_engine.py
│   └── permissions.py
│
├── crud/                      # 数据访问层（读写数据库、返回 DataFrame / 抛异常）
│   ├── inventory.py
│   ├── orders.py
│   ├── planning.py
│   ├── logs.py
│   ├── contracts.py
│   └── users.py
│
├── utils/                     # 通用工具（解析、格式化、导入辅助）
│   ├── formatters.py
│   └── parsers.py
│
└── views/                     # 页面视图层（纯 UI 为主）
    ├── auth.py
    ├── boss_planning.py
    ├── common.py              # 已弃用，仅保留兼容说明
    ├── components.py
    ├── home.py
    ├── inbound.py
    ├── inbound_modules.py
    ├── log_viewer.py
    ├── machine_archive.py
    ├── machine_edit.py
    ├── production.py
    ├── query.py
    ├── router.py              # 第二轮新增：页面路由表
    ├── sales_alloc.py
    ├── sales_create.py
    ├── ship_confirm.py
    ├── sidebar.py
    ├── styles.py
    └── user_management.py
```

---

## 模块职责说明

### `app.py`

唯一主入口。现在它只做 4 件事：

1. 调用 `initialize_app()` 完成应用初始化
2. 未登录时展示登录页
3. 登录后渲染左侧边栏
4. 根据 `st.session_state.page` 分发页面

这就是主入口应该有的复杂度。

### `config.py`

集中保存所有**跨模块共享、变化频率低**的配置：

- MySQL 连接参数
- 默认用户
- 默认角色权限
- 机型排序 `CUSTOM_MODEL_ORDER`
- 页面功能映射 `FUNC_MAP`
- Plotly / OCR / openpyxl 等可选依赖开关
- 全局 CSS
- 物理文件目录

后续新增新的“系统级常量”，优先放这里。

### `database.py`

数据库基础设施层，只处理：

- 获取 SQLAlchemy Engine
- 初始化表结构
- 注入默认用户与默认权限

后续不要把业务 SQL 散着写在页面里，尽量经由 `crud/` 间接访问。

### `crud/`

这是你后续最值得继续加固的一层。

原则：

- 这里只做**数据读写**
- 返回 `DataFrame`、普通值、布尔值、元组
- 或者抛出异常
- **绝对不要写 `st.error()`、`st.success()`**

当前模块大致分工：

- `crud.inventory.py`：库存主表、待入库暂存表
- `crud.orders.py`：订单、新建订单、配货、撤回等
- `crud.planning.py`：排产、规划记录
- `crud.logs.py`：流水日志
- `crud.contracts.py`：合同文件记录
- `crud.users.py`：用户审核与管理

### `core/`

放“比 CRUD 更接近业务，又不属于页面显示”的逻辑：

- `auth.py`：登录、注册、Session 初始化
- `permissions.py`：角色权限校验
- `file_manager.py`：合同文件保存、删除、清理
- `ocr_engine.py`：OCRProcessor 的专属归宿
- `navigation.py`：页面跳转
- `bootstrap.py`：应用启动编排

### `utils/`

放纯工具函数：

- `formatters.py`：机型排序、格式处理
- `parsers.py`：跟踪单解析、导入 payload、差异对比等

### `views/`

这里只应该关心三件事：

- 页面布局
- 表单输入
- 结果展示

页面不要承担过多的数据清洗、数据库写入、文件系统操作。

---

## 当前页面与文件映射

| 页面 | 文件 | 主要职责 |
|---|---|---|
| 登录/注册 | `views/auth.py` | 登录表单、注册申请 |
| 首页 | `views/home.py` | 功能入口聚合 |
| 左侧边栏 | `views/sidebar.py` | 当前用户信息、导航 |
| 生产统筹 | `views/boss_planning.py` | 合同审核、排产、订单资源分配 |
| 合同管理 | `views/production.py` | 未来合同录入、文件上传 |
| 机台编辑 | `views/machine_edit.py` | 修改库存机台信息 |
| 机台档案 | `views/machine_archive.py` | 机器档案文件管理 |
| 销售下单 | `views/sales_create.py` | 手动下单、导入规划合同 |
| 智能配货 | `views/sales_alloc.py` | 订单配货、撤回 |
| 发货复核 | `views/ship_confirm.py` | 发货前确认与回退 |
| 成品入库 | `views/inbound.py` | 手动入库、跟踪单导入、自动流水号 |
| 查询看板 | `views/query.py` | 库存全景与统计图表 |
| 日志 | `views/log_viewer.py` | 查看操作流水 |
| 用户管理 | `views/user_management.py` | 审核注册用户、管理角色 |

---

## 依赖方向约定（非常重要）

后续继续开发时，请尽量遵守这条依赖链：

```text
views  ->  core / crud / utils / config
core   ->  crud / database / config / utils
crud   ->  database / config
utils  ->  config（可选）
database -> config
```

请尽量避免反向依赖：

- `crud` 不要 import `views`
- `database` 不要 import `views`
- `core` 不要 import `views` 中的页面代码

这样能最大程度避免循环导入。

---

## 启动流程

当前启动流程如下：

1. `streamlit run app.py`
2. `app.py` 调用 `core.bootstrap.initialize_app()`
3. 初始化页面配置、样式、目录、Session、数据库、合同清理
4. 未登录则渲染 `views/auth.py`
5. 登录后渲染 `views/sidebar.py`
6. 进入 `views/router.py`，根据 `st.session_state.page` 找到对应页面函数
7. 执行页面渲染逻辑

---

## 数据库表说明

当前初始化逻辑会自动创建以下表：

- `finished_goods_data`：成品库存主表
- `sales_orders`：订单表
- `factory_plan`：合同 / 排产总表
- `transaction_log`：操作日志表
- `planning_records`：订单规划记录
- `contract_records`：合同附件记录
- `audit_log`：审计日志
- `users`：用户表
- `role_permissions`：角色权限表
- `shipping_history`：已出库历史表
- `plan_import`：待入库暂存表

这些表都由 `database.init_mysql_tables()` 初始化。

---

## 环境变量说明

项目从环境变量读取 MySQL 等配置。已附带 `.env.example` 作为示例。

主要变量：

```env
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=123456
MYSQL_DB=rjfinshed
ADMIN_PASSWORD=888
```

> 注意：当前代码没有强制加载 `.env` 文件。也就是说，你可以：
>
> - 直接在系统环境变量里设置
> - 或者自己后续引入 `python-dotenv` 来自动加载 `.env`

---

## 运行方式

### 1. 安装基础依赖

至少需要这些：

- `streamlit`
- `pandas`
- `sqlalchemy`
- `pymysql`

推荐依赖（按你原始代码推断）：

- `plotly`
- `openpyxl`
- `mammoth`
- `paddleocr`
- `pdfplumber`
- `python-docx`

### 2. 启动
=======
这是基于原始 `V6.py` 拆出来的一版过渡性重构骨架：

- `config.py`：环境变量、常量、目录、权限、机型排序、全局 CSS
- `database.py`：`get_engine()` / `init_mysql_tables()`
- `crud/`：数据库读写
- `core/`：认证、权限、文件管理、OCR、导航
- `utils/`：解析器与格式化器
- `views/`：侧边栏、登录页、各页面渲染函数
- `app.py`：唯一入口

## 说明

这版重构优先保证“拆分落地”和“低风险迁移”，所以 `views/common.py` 暂时保留了聚合导入，便于先把巨型单文件拆开，再继续做第二轮精修。

## 启动
>>>>>>> 88b8b5966418ed53f8a893e327b91727f9c01707

```bash
streamlit run app.py
```
<<<<<<< HEAD

### 3. 首次启动行为

首次运行会自动：

- 创建 MySQL 表
- 写入默认用户
- 写入默认角色权限
- 创建本地文件目录

---

## 默认账号

默认账号写在 `config.py -> DEFAULT_USERS` 中。当前默认用户包括：

- `boss`
- `admin`
- `sales`
- `prod`

建议在生产环境里：

- 尽快修改默认密码
- 将敏感信息从代码迁到环境变量或配置中心

---

## 已知可继续优化的地方

这版已经比第一轮清晰很多，但还不是“最终版”。下面这些是你第三轮最值得继续做的：

### 1）继续把页面中的业务规则下沉

现在很多页面虽然不再依赖 `views.common`，但仍然包含较多业务判断。

典型方向：

- 把订单资源匹配逻辑再下沉到 `core/services`
- 把页面里大段 DataFrame 处理逻辑下沉到 `core/` 或 `utils/`
- 让 `views/*.py` 更接近“输入 -> 调用 -> 显示结果”的薄层

### 2）把 `core.auth.py` 中的 `streamlit` 依赖继续下沉/隔离

当前 `init_session_state()` 仍依赖 `st.session_state`，这在 Streamlit 项目里是可以接受的，但如果未来你想做更可测试的结构，可以进一步把：

- 认证逻辑
- Session 管理
- 页面状态管理

拆得更细。

### 3）对 CRUD 做更细粒度切分

例如 `crud.orders.py` 目前既包含：

- 订单表读写
- 配货动作
- 撤回动作

未来可以进一步拆成：

- `crud/orders.py`
- `core/order_service.py`
- `core/allocation_service.py`

这样语义会更清楚。

### 4）补自动化测试

目前这版主要完成的是**结构重构**，不是测试重构。

建议后续优先补：

- `utils/parsers.py` 的纯函数测试
- `crud/*.py` 的数据库读写测试
- 关键业务函数的回归测试

### 5）引入统一错误处理策略

现在有些 CRUD 是：

- 返回空 DataFrame
- 有些是抛异常
- 有些是打印日志

未来最好统一成一种规范，例如：

- 查询类函数：失败时抛 `RuntimeError`
- 页面层 `try/except` 后用 `st.error()` 呈现

这样最利于长期维护。

---

## 新增页面的推荐写法

假设你要新增一个“报表分析”页面，建议按下面步骤做：

### 第一步：创建视图文件

```python
# views/reports.py
import streamlit as st

from core.navigation import go_home
from core.permissions import check_access
from crud.inventory import get_data


def render_reports():
    check_access('REPORTS')
    c_back, c_title = st.columns([2, 8])
    with c_back:
        st.button('⬅️ 返回', on_click=go_home, use_container_width=True)
    with c_title:
        st.header('📈 报表分析')

    df = get_data()
    st.dataframe(df, use_container_width=True)
```

### 第二步：注册路由

在 `views/router.py` 中增加：

```python
from views.reports import render_reports

ROUTES = {
    # ...
    'reports': render_reports,
}
```

### 第三步：增加入口按钮

在 `config.py -> FUNC_MAP` 或首页/侧边栏中增加按钮映射。

---

## 推荐开发约定

### 约定 1：页面函数统一命名

页面渲染函数统一命名为：

- `render_xxx()`

这样路由层会很直观。

### 约定 2：CRUD 不直接弹提示

错误处理规则建议统一成：

- `crud/`：抛异常或返回值
- `views/`：`st.success / st.warning / st.error`

### 约定 3：页面状态仍优先使用 `st.session_state`

在 Streamlit 项目里，这是合理且常见的做法。

但建议：

- 页面状态键名统一前缀
- 避免不同页面共用含义模糊的 key

例如：

- `boss_selected_id`
- `archive_sn_search`
- `contract_models`

都比 `selected_id`、`temp` 这种名字清晰得多。

### 约定 4：复杂业务逻辑先抽函数，再考虑抽类

不要一上来就强行面向对象。

更适合这类系统的顺序通常是：

1. 先把大段逻辑抽成函数
2. 再看是否存在明显的状态聚合
3. 只有确实需要长期维护状态时再抽类

---

## 这版适合怎么继续用

你现在可以把这版当成：

- **可运行的重构中间态**
- **后续继续精修的主分支基础**
- **团队协作时更容易分工的目录结构**

最推荐的后续顺序是：

1. 先用这版替代原始巨型 `V6.py`
2. 每次只继续精修一个页面
3. 每精修一个页面，就顺便把其中的业务逻辑继续下沉
4. 最后再统一做测试和错误处理规范化

---

## 本次重构的落地结论

如果只用一句话概括这版的价值，那就是：

> **它已经把“巨无霸单文件 + 页面直接到处拿依赖”的形态，推进到了“有边界、有入口、有路由、有目录分层”的可维护结构。**

这还不是终点，但已经是一个适合继续开发的起点。
=======
>>>>>>> 88b8b5966418ed53f8a893e327b91727f9c01707
