# -*- coding: utf-8 -*-
"""交付文档守卫：文档必须与真相源（映射表 / 客户指标表）一致。

覆盖三份客户交付文档：

  1. ``docs/交付-指标口径说明.md``          —— 覆盖全部 87 个生效指标
  2. ``docs/交付-不可提供指标清单.md``      —— 恰好 3 条，与排除清单的
     ``reason=unsupported`` 完全一致
  3. ``docs/交付-客户指标英文名核对清单.md`` —— 每条都能在客户 xlsx 里找到对应行

**防恒绿**：文件末尾的 ``test_guard_*`` 系列用「合成违规样本」验证守卫本身
会变红 —— 守卫的判据被放松时必须失败。
"""
import json
import os
import re

import pytest

import build_delivery_docs as builder
from conftest import REPO_ROOT

DOCS_DIR = os.path.join(REPO_ROOT, 'docs')
SPEC_PATH = os.path.join(DOCS_DIR, '交付-指标口径说明.md')
UNSUPPORTED_PATH = os.path.join(DOCS_DIR, '交付-不可提供指标清单.md')
OCR_PATH = os.path.join(DOCS_DIR, '交付-客户指标英文名核对清单.md')

METRIC_RULES_PATH = os.path.join(
    REPO_ROOT, 'tpops_kafka_collector', 'mapping', 'data', 'metric_rules.json')

#: 口径说明里必须非空的列
#:
#: 「刷新周期」已拆成两列：**声明周期**（客户指标库 / 映射表 ``collect_cycle``）
#: 与**实际产出频率**（所属通道的节奏 —— 与生产调度代码同源）。
#: 两列都要有，且**不能相等当成一回事**：声明周期只是口径声明，不是产出闸门。
REQUIRED_SPEC_COLUMNS = ('客户指标英文名', '中文名', '取数通道', '来源',
                         '取值规则', '声明周期', '实际产出频率',
                         '资源粒度', '归集口径')

#: 像指标名的串（用于「文档里不能出现不存在的指标」）
METRIC_CODE_RE = re.compile(r'`((?:gaussdb|GAUSSDB)[^`]*)`')


EXCLUDED_PATH = os.path.join(
    REPO_ROOT, 'tpops_kafka_collector', 'mapping', 'data', 'excluded_metrics.json')


@pytest.fixture(scope='module')
def rules():
    """映射表的 87 条生效规则。"""
    with open(METRIC_RULES_PATH, 'r', encoding='utf-8') as handle:
        return json.load(handle)['rules']


@pytest.fixture(scope='module')
def excluded():
    """排除清单（14 条：11 条重复 + 3 条无法提供）。"""
    with open(EXCLUDED_PATH, 'r', encoding='utf-8') as handle:
        return json.load(handle)


@pytest.fixture(scope='module')
def enabled_codes(rules):
    """87 个生效指标的 metric_code。"""
    return set(rule['metric_code'] for rule in rules if rule.get('enabled'))


@pytest.fixture(scope='module')
def unsupported_codes(excluded):
    """3 个无法提供指标的 metric_code（来自排除清单，不是映射表）。"""
    return set(
        entry['metric_code'] for entry in excluded['excluded']
        if entry.get('reason') == 'unsupported'
    )


@pytest.fixture(scope='module')
def scope(rules):
    """客户指标库全量（生效口径 + 被排除的）—— OCR 检测器跑在它上面。"""
    return builder.customer_scope_rules(rules, builder.load_customer_table())


def _read(path):
    """读文档；不存在就给出明确失败原因。"""
    assert os.path.exists(path), '交付文档不存在：%s（先跑 `python tools/build_delivery_docs.py`）' % path
    with open(path, 'r', encoding='utf-8') as handle:
        return handle.read()


# --------------------------------------------------------------------------
# 判据（既用于真实文档，也用于合成违规样本 —— 所以被放松时会真的变红）
# --------------------------------------------------------------------------
def check_spec_coverage(text, expected_codes):
    """口径说明必须恰好覆盖 expected_codes —— 返回 (缺失, 多余)。"""
    rows = builder.parse_metric_spec_rows(text)
    listed = set()
    for row in rows:
        code = row['客户指标英文名'].strip().strip('`')
        assert code, '口径说明里有空指标名：%r' % row
        listed.add(code)
    return sorted(expected_codes - listed), sorted(listed - expected_codes)


def check_unsupported_list(text, expected_codes):
    """不可提供清单必须恰好等于 expected_codes —— 返回 (缺失, 多余)。"""
    rows = builder.parse_unsupported_rows(text)
    listed = set(row['客户指标英文名'].strip().strip('`') for row in rows)
    return sorted(expected_codes - listed), sorted(listed - expected_codes)


def check_ocr_entries(text, known_codes):
    """OCR 清单里的每条都要在 known_codes 里 —— 返回多出来的。"""
    rows = builder.parse_ocr_rows(text)
    unknown = []
    for row in rows:
        code = row['客户表里的英文名'].strip().strip('`')
        if code not in known_codes:
            unknown.append(code)
    return unknown


def check_spec_rows_complete(text):
    """口径说明每行的必需列都不能是空 —— 返回有问题的行。"""
    problems = []
    for row in builder.parse_metric_spec_rows(text):
        for column in REQUIRED_SPEC_COLUMNS:
            value = (row.get(column) or '').strip()
            if not value:
                problems.append('%s / %s' % (row.get('客户指标英文名'), column))
    return problems


def _append_table_row(text, mark, row):
    """往标记后的表格末尾插一行（列数不变，避免列错位）。"""
    lines = text.splitlines()
    start = lines.index(mark)
    last = None
    for index in range(start + 1, len(lines)):
        stripped = lines[index].strip()
        if stripped.startswith('|'):
            last = index
            continue
        if stripped == '':
            if last is not None:
                break
            continue
        break
    assert last is not None, '标记 %s 后面没有表格' % mark
    lines.insert(last + 1, row)
    return '\n'.join(lines)


# --------------------------------------------------------------------------
# 1. 口径说明
# --------------------------------------------------------------------------
def test_metric_spec_covers_all_enabled_metrics(enabled_codes):
    """口径说明覆盖全部 98 个可提供指标：一个不漏、一个不多。"""
    text = _read(SPEC_PATH)
    missing, extra = check_spec_coverage(text, enabled_codes)
    assert not missing, '口径说明缺少这些指标：%s' % '、'.join(missing)
    assert not extra, '口径说明里出现了不该有的指标：%s' % '、'.join(extra)
    assert len(enabled_codes) == 87, '生效指标数不是 87，映射表变了？'


def test_metric_spec_rows_have_all_columns():
    """每个指标的口径行字段齐全（取值规则、刷新周期不能空）。"""
    problems = check_spec_rows_complete(_read(SPEC_PATH))
    assert not problems, '这些行缺字段：%s' % '；'.join(problems)


def test_metric_spec_only_mentions_known_metrics(rules, enabled_codes):
    """文档里出现的指标名确实存在于映射表。"""
    text = _read(SPEC_PATH)
    known = set(rule['metric_code'] for rule in rules)
    # 平台侧的指标名（source.tpops_source）也是映射表里的合法标识
    for rule in rules:
        tpops_source = (rule.get('source') or {}).get('tpops_source')
        if tpops_source:
            known.add(tpops_source)
    rows = builder.parse_metric_spec_rows(text)
    suggestions = set()
    for row in rows:
        suggestion = row.get('建议的正确拼写', '').strip().strip('`')
        if suggestion:
            suggestions.add(suggestion)
    for code in METRIC_CODE_RE.findall(text):
        if code in known or code in suggestions:
            continue
        assert False, '口径说明里出现了映射表里没有的指标名：%s' % code


def test_metric_spec_documents_node_scoped_semantics():
    """node_scoped 归集口径必须单独讲清楚（节点级 / 实例级 / 多 master 去重）。"""
    text = _read(SPEC_PATH)
    for keyword in ('node_scoped', 'master_view', 'master CN', '去重', 'node_id'):
        assert keyword in text, '口径说明里没有讲清「%s」' % keyword


def test_metric_spec_value_rules_are_described(rules):
    """取值规则列不能退化成「None」或「规则类型 X」（生成脚本重构踩过的坑）。"""
    known_kinds = set((rule.get('value_rule') or {}).get('type')
                      for rule in rules if rule.get('enabled'))
    known_kinds.discard(None)
    # 用**刚生成**的内容来查（而不是磁盘上的文件），这样生成逻辑一坏就会变红
    generated = builder.build_all()[SPEC_PATH]
    for row in builder.parse_metric_spec_rows(generated):
        text = row['取值规则']
        assert 'None' not in text, '取值规则退化成了 None：%s' % row['客户指标英文名']
        assert not text.startswith('规则类型 '), \
            '取值规则退化成了兜底文案：%s' % row['客户指标英文名']
    for kind in known_kinds:
        assert ('`%s`' % kind) in generated, '文档里没有出现规则类型 %s' % kind
    assert len(known_kinds) == 13, '规则类型数不是 13，映射表变了？'


# --------------------------------------------------------------------------
# 2. 不可提供清单
# --------------------------------------------------------------------------
def test_unsupported_doc_lists_exactly_the_unsupported_exclusions(unsupported_codes):
    """不可提供清单 = 3 条，且与**排除清单**的 ``reason=unsupported`` 完全一致。

    被排除的指标根本不在生效映射表里 —— 不能再去映射表里找「``enabled=false``
    的规则」。
    """
    text = _read(UNSUPPORTED_PATH)
    missing, extra = check_unsupported_list(text, unsupported_codes)
    assert not missing, '不可提供清单缺少：%s' % '、'.join(missing)
    assert not extra, '不可提供清单多出：%s' % '、'.join(extra)
    assert len(unsupported_codes) == 3, '不可提供指标数不是 3，排除清单变了？'


def test_unsupported_doc_explains_reason_and_workaround(excluded):
    """每条都要有「为什么给不了」和「能不能变通」，且理由来自排除清单。"""
    text = _read(UNSUPPORTED_PATH)
    for entry in excluded['excluded']:
        if entry.get('reason') != 'unsupported':
            continue
        code = entry['metric_code']
        assert code in text, '不可提供清单里没有 %s' % code
        reason = entry.get('detail') or ''
        assert reason and reason in text, \
            '不可提供清单没有引用排除清单里的理由：%s' % code
        assert '**能不能变通**' in text, '不可提供清单没有写变通方案'


def test_unsupported_doc_states_the_build_time_exclusion(excluded):
    """要写明处置方式是**构建期排除**，运行期没有丢弃分支。"""
    text = _read(UNSUPPORTED_PATH)
    assert 'dropped_unsupported' in text
    assert '构建期' in text
    assert '恒为 **0**' in text
    assert '已从生效口径移除' in text or '从生效口径移除' in text


# --------------------------------------------------------------------------
# 3. OCR 核对清单
# --------------------------------------------------------------------------
def test_ocr_entries_exist_in_customer_xlsx():
    """OCR 清单里的每条都能在客户 xlsx 里找到对应行（防止凭空编）。"""
    rows = builder.load_customer_table()
    known = set(row['metric_code'] for row in rows)
    text = _read(OCR_PATH)
    unknown = check_ocr_entries(text, known)
    assert not unknown, 'OCR 清单里有客户表里不存在的指标名：%s' % '、'.join(unknown)
    assert builder.parse_ocr_rows(text), 'OCR 清单是空的'


def test_ocr_checklist_matches_generator_review():
    """OCR 清单与生成脚本的人工复核表一致（文档不能漏条、不能多写）。"""
    text = _read(OCR_PATH)
    rows = builder.parse_ocr_rows(text)
    listed = [row['客户表里的英文名'].strip().strip('`') for row in rows]
    assert listed == list(builder.OCR_REVIEW.keys()), \
        'OCR 清单与 tools/build_delivery_docs.py 的复核表不一致'


def test_ocr_checklist_has_suggestion_and_impact():
    """每条都要有建议拼写与影响面。"""
    text = _read(OCR_PATH)
    for row in builder.parse_ocr_rows(text):
        assert row['建议的正确拼写'].strip(), '缺少建议拼写：%s' % row
        assert row['影响面'].strip(), '缺少影响面：%s' % row
        assert '取值' in row['影响面'], '影响面要说明是否影响取值：%s' % row


def test_detector_finds_known_ocr_typos(scope):
    """自动检测器要能抓到已知的 OCR 错字。"""
    findings = builder.detect_ocr_findings(scope)
    pairs = set((item['metric_code'], item['token']) for item in findings)
    assert ('GAUSSDB.Number_of_ansactions_mmitted_r_cond', 'ansactions') in pairs
    assert ('GAUSSDB.Number_of_ansactions_mmitted_r_cond', 'mmitted') in pairs
    assert ('GAUSSDB.Number_of_llback_ansactions_r_cond', 'llback') in pairs
    assert ('gaussdb.{#NAME}.number.Of.dex.ples.ad', 'dex') in pairs
    assert ('GAUSSDB.Checkpoint_count_eckpoint_gments', 'gments') in pairs


def test_ocr_review_entries_are_corroborated(scope):
    """人工复核表里的每条，检测器都要能给出候选（复核表不能凭空调）。

    检测器跑在**客户指标库全量**上（生效口径 + 被排除的）—— 核对清单的对象
    是客户的指标表。
    """
    findings = builder.detect_ocr_findings(scope)
    candidates = {}
    for item in findings:
        candidates.setdefault(item['metric_code'], set()).add(item['token'])
    for code, items in builder.OCR_REVIEW.items():
        for bad, _good, _why in items:
            for token in builder.code_tokens(bad):
                assert token in candidates.get(code, set()), \
                    '复核表里的 %s / %s 没有检测器佐证' % (code, bad)


def test_other_docs_only_mention_known_metrics(rules, excluded):
    """不可提供清单与 OCR 清单里出现的指标名也必须已知。

    「已知」= 生效映射表里的指标码 + **排除清单**里的指标码
    （被排除的指标也在客户表里，OCR 清单仍会提到它们）。
    """
    known = set(rule['metric_code'] for rule in rules)
    known.update(entry['metric_code'] for entry in excluded['excluded'])
    for rule in rules:
        tpops_source = (rule.get('source') or {}).get('tpops_source')
        if tpops_source:
            known.add(tpops_source)
    for path in (UNSUPPORTED_PATH, OCR_PATH):
        text = _read(path)
        suggestions = set()
        if builder.TABLE_MARKS['ocr'] in text:
            for row in builder.parse_markdown_table(text, builder.TABLE_MARKS['ocr']):
                suggestion = (row.get('建议的正确拼写') or '').strip().strip('`')
                if suggestion:
                    suggestions.add(suggestion)
        for code in METRIC_CODE_RE.findall(text):
            if '…' in code:
                # 族模式（带省略号）不是具体指标名，跳过
                continue
            assert code in known or code in suggestions, \
                '%s 里出现了映射表里没有的指标名：%s' % (os.path.basename(path), code)


# --------------------------------------------------------------------------
# 3.5 三份文档的条数与生效映射表一致（防文档与代码漂移）
# --------------------------------------------------------------------------
def test_doc_counts_match_the_effective_mapping(rules, unsupported_codes, excluded):
    """三份交付文档的条数必须与**生效映射表**对得上。

    - 口径说明：每个生效指标恰好一行；
    - 不可提供清单：与排除清单里 ``reason=unsupported`` 的条数一致；
    - OCR 核对清单：条目只针对客户指标库全量（生效口径 + 被排除的）。
    """
    effective = len(rules)
    assert effective == 87, '生效指标数不是 87，映射表变了？'

    spec_rows = builder.parse_metric_spec_rows(_read(SPEC_PATH))
    assert len(spec_rows) == effective, \
        '口径说明 %d 行 ≠ 生效指标 %d 条' % (len(spec_rows), effective)

    unsupported_rows = builder.parse_unsupported_rows(_read(UNSUPPORTED_PATH))
    assert len(unsupported_rows) == len(unsupported_codes) == 3, \
        '不可提供清单 %d 行 ≠ 排除清单的 3 条' % len(unsupported_rows)

    scope = builder.customer_scope_rules(rules, builder.load_customer_table())
    assert len(scope) == effective + excluded['counts']['total'], \
        '客户指标库全量条数不对：%d' % len(scope)
    listed = set(
        row['客户表里的英文名'].strip().strip('`')
        for row in builder.parse_ocr_rows(_read(OCR_PATH))
    )
    scope_codes = set(rule['metric_code'] for rule in scope)
    assert listed <= scope_codes, \
        'OCR 清单里有客户指标库之外的指标名：%s' % sorted(listed - scope_codes)


# --------------------------------------------------------------------------
# 4. 文档与生成脚本同步（重跑输出不变）
# --------------------------------------------------------------------------
def test_docs_match_generator_output():
    """文档必须与生成脚本的输出一致（防止文档被手改后与映射表脱节）。"""
    outputs = builder.build_all()
    for path, content in outputs.items():
        assert _read(path) == content, '%s 与生成脚本的输出不一致' % os.path.basename(path)


def test_generator_is_idempotent():
    """生成脚本幂等：连跑两次输出完全一样。"""
    first = builder.build_all()
    second = builder.build_all()
    assert first == second


# --------------------------------------------------------------------------
# 5. 防恒绿：合成违规样本，守卫必须抓到
# --------------------------------------------------------------------------
def test_guard_detects_missing_metric_row(enabled_codes):
    """删掉一行指标 → 守卫必须报「缺失」。"""
    text = _read(SPEC_PATH)
    victim = sorted(enabled_codes)[0]
    broken = '\n'.join(
        line for line in text.splitlines()
        if ('`%s`' % victim) not in line)
    missing, _extra = check_spec_coverage(broken, enabled_codes)
    assert victim in missing, '守卫没抓到被删掉的指标 %s' % victim


def test_guard_detects_extra_metric_row(enabled_codes):
    """塞进一个映射表里没有的指标 → 守卫必须报「多余」。"""
    text = _read(SPEC_PATH)
    broken = _append_table_row(
        text, builder.TABLE_MARKS['metrics'],
        '| 999 | `gaussdb.Not_A_Real_Metric` | 假指标 | 命令中心SQL | 假的 | 假的 | '
        '5 分钟 | 实例级 | 假的 | — | — |')
    _missing, extra = check_spec_coverage(broken, enabled_codes)
    assert 'gaussdb.Not_A_Real_Metric' in extra, '守卫没抓到多余的指标'


def test_guard_detects_incomplete_row():
    """把某行的「取值规则」清空 → 守卫必须报「缺字段」。"""
    text = _read(SPEC_PATH)
    lines = text.splitlines()
    broken_lines = []
    patched = False
    for line in lines:
        if not patched and line.startswith('| 1 |') and '`' in line:
            cells = line.split('|')
            # 列：| # | 英文名 | 中文名 | 通道 | 来源 | 取值规则 | ...
            cells[6] = '  '
            line = '|'.join(cells)
            patched = True
        broken_lines.append(line)
    assert patched, '没找到可改的行，测试本身失效'
    problems = check_spec_rows_complete('\n'.join(broken_lines))
    assert problems, '守卫没抓到缺字段的行'


def test_guard_detects_wrong_unsupported_list(unsupported_codes):
    """不可提供清单里塞进一条可提供指标 → 守卫必须报「多余」。"""
    text = _read(UNSUPPORTED_PATH)
    broken = _append_table_row(
        text, builder.TABLE_MARKS['unsupported'],
        '| 999 | `gaussdb.Tps` | 假不可提供 | 假的 | 假的 | 假的 |')
    missing, extra = check_unsupported_list(broken, unsupported_codes)
    assert 'gaussdb.Tps' in extra, '守卫没抓到多余的不可提供指标'
    assert not missing, '守卫误报了缺失'


def test_guard_detects_unknown_ocr_entry():
    """OCR 清单里编一个客户表里没有的名字 → 守卫必须抓到。"""
    text = _read(OCR_PATH)
    broken = _append_table_row(
        text, builder.TABLE_MARKS['ocr'],
        '| 999 | `gaussdb.Fake_Metric_Name` | 同名 | 假问题 | `x` | 影响 |')
    unknown = check_ocr_entries(broken, set())
    assert 'gaussdb.Fake_Metric_Name' in unknown, '守卫没抓到编造的 OCR 条目'


# --------------------------------------------------------------------------
# 6. 部署运行说明 + .env.example：环境变量必须与 config.py 一致
#
#    合同要求：「环境变量表与 tpops_kafka_collector/config.py 实际读取的
#    变量一致」。下面 3 条判据既用于真实文件，也用于合成违规样本 ——
#    判据被放松时必须变红（见本节末尾的 test_guard_detects_* ）。
# --------------------------------------------------------------------------
DEPLOY_DOC_PATH = os.path.join(DOCS_DIR, '交付-部署运行说明.md')
ENV_EXAMPLE_PATH = os.path.join(REPO_ROOT, '.env.example')
CONFIG_PATH = os.path.join(REPO_ROOT, 'tpops_kafka_collector', 'config.py')
#: 日志相关的配置解析单独成文件（config.py 贴着 400 行红线）—— 一并扫描。
LOG_CONFIG_PATH = os.path.join(REPO_ROOT, 'tpops_kafka_collector', 'log_config.py')
CONFIG_PATHS = (CONFIG_PATH, LOG_CONFIG_PATH)

#: 凭据类变量 —— .env.example 里必须留空（禁硬编码口令）。
SECRET_ENV_VARS = ('TPOPS_PASSWORD',)

#: config.py 应当读取的环境变量数（清单见开发方案 §5.5）。
#: wave-2 起日志相关的四项（LOG_DIR / LOG_PATTERN / LOG_MAX_BYTES /
#: LOG_RETENTION_DAYS）由 log_config.py 解析 —— 一并计入。
EXPECTED_ENV_VAR_COUNT = 34


def config_env_vars():
    """配置层实际读取的环境变量名集合（config.py + log_config.py）。"""
    names = set()
    for path in CONFIG_PATHS:
        names |= set(re.findall(r"env,\s*'([A-Z][A-Z0-9_]+)'", _read(path)))
    return names


def _normalize_default(value):
    """把默认值归一化成可比较的字符串（两边走同一条归一化路径）。"""
    text = str(value).strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in ("'", '"'):
        text = text[1:-1]
    text = text.strip().strip('`').strip()
    lowered = text.lower()
    if lowered in ('true', 'false'):
        return lowered
    try:
        number = float(text)
    except ValueError:
        return text
    if number == int(number):
        return str(int(number))
    return str(number)


def config_env_defaults():
    """config.py 里每个变量的默认值 → {变量名: 归一化默认值}。

    取不到默认值的（必填项，``_require`` / ``_servers``）不在结果里。
    """
    text = _read(CONFIG_PATH)
    constants = {}
    match = re.search(r'PLATFORM_LIMIT_MAX\s*=\s*(\d+)', text)
    if match:
        constants['PLATFORM_LIMIT_MAX'] = match.group(1)
    for name in ('DEFAULT_LOG_MAX_BYTES', 'DEFAULT_LOG_RETENTION_DAYS'):
        match = re.search(name + r'\s*=\s*(\d+)', text)
        if match:
            constants[name] = match.group(1)
    defaults = {}
    for name, raw in re.findall(
            r"env,\s*'([A-Z][A-Z0-9_]+)'\s*,\s*([^,)\n]+)", text):
        token = raw.strip()
        if token.startswith(("'", '"')):
            defaults[name] = token
        elif token in constants:
            defaults[name] = constants[token]
        elif token in ('True', 'False'):
            defaults[name] = token
        elif re.match(r'^-?\d+(\.\d+)?$', token):
            defaults[name] = token
    return dict((name, _normalize_default(value))
                for name, value in defaults.items())


def parse_deploy_env_table(text):
    """部署说明里的环境变量表 → {变量名: 单元格列表}。

    只认**表头为变量名**的行（首格形如 `` `TPOPS_USER` ``）——
    文档里其它表格（前置条件、排障…）的首格是中文，不会被误认。
    """
    rows = {}
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith('|'):
            continue
        cells = [cell.strip() for cell in stripped.strip('|').split('|')]
        if not cells:
            continue
        name = cells[0].strip().strip('`').strip()
        if re.match(r'^[A-Z][A-Z0-9_]+$', name):
            rows[name] = cells
    return rows


def check_deploy_env_table(text, expected):
    """部署说明的环境变量表必须与 expected 完全一致 —— 返回 (缺失, 多余)。"""
    listed = set(parse_deploy_env_table(text))
    return sorted(expected - listed), sorted(listed - expected)


def env_example_vars(text):
    """.env.example 里声明的变量名集合。"""
    return set(re.findall(r'^([A-Z][A-Z0-9_]+)=', text, re.M))


def check_env_example_covers(text, expected):
    """.env.example 必须覆盖 expected 的全部变量 —— 返回缺失的。"""
    return sorted(expected - env_example_vars(text))


def check_env_example_secrets(text):
    """凭据类变量必须留空 —— 返回有值的变量名。"""
    problems = []
    for name in SECRET_ENV_VARS:
        match = re.search(r'^%s=(.*)$' % name, text, re.M)
        if match is None:
            continue
        if match.group(1).strip():
            problems.append(name)
    return problems


def test_deploy_doc_env_table_matches_config():
    """部署说明的环境变量表与 config.py 实际读取的变量完全一致（不多不少）。"""
    expected = config_env_vars()
    assert len(expected) == EXPECTED_ENV_VAR_COUNT, \
        'config.py 读取的环境变量数不是 %d：%s' % (
            EXPECTED_ENV_VAR_COUNT, sorted(expected))
    missing, extra = check_deploy_env_table(_read(DEPLOY_DOC_PATH), expected)
    assert not missing, '部署说明的环境变量表缺少：%s' % '、'.join(missing)
    assert not extra, '部署说明的环境变量表多了：%s' % '、'.join(extra)


def test_env_example_covers_every_config_var():
    """.env.example 覆盖 config.py 的全部变量（可以多，不许少）。"""
    expected = config_env_vars()
    missing = check_env_example_covers(_read(ENV_EXAMPLE_PATH), expected)
    assert not missing, '.env.example 缺少这些变量：%s' % '、'.join(missing)


def test_env_example_has_no_hardcoded_password():
    """.env.example 里凭据类变量必须留空（禁硬编码口令）。"""
    problems = check_env_example_secrets(_read(ENV_EXAMPLE_PATH))
    assert not problems, \
        '.env.example 里这些凭据变量有值：%s' % '、'.join(problems)


def test_deploy_doc_defaults_match_config():
    """部署说明表里的默认值必须与 config.py 一致。"""
    defaults = config_env_defaults()
    assert len(defaults) >= 25, \
        '默认值解析失效（只解析出 %d 个，判据可能已恒绿）' % len(defaults)
    rows = parse_deploy_env_table(_read(DEPLOY_DOC_PATH))
    problems = []
    for name, expected in sorted(defaults.items()):
        cells = rows.get(name)
        if not cells or len(cells) < 2:
            problems.append('%s（表里没有这一行或列不全）' % name)
            continue
        actual = _normalize_default(cells[1])
        if actual != expected:
            problems.append(
                '%s：文档写 %s，config.py 是 %s' % (name, actual, expected))
    assert not problems, '；'.join(problems)


def test_env_example_values_match_config_defaults():
    """.env.example 里每个变量的取值必须与 config.py 的默认值一致。"""
    defaults = config_env_defaults()
    assert len(defaults) >= 25, \
        '默认值解析失效（只解析出 %d 个，判据可能已恒绿）' % len(defaults)
    text = _read(ENV_EXAMPLE_PATH)
    problems = []
    for name, expected in sorted(defaults.items()):
        match = re.search(r'^%s=(.*)$' % name, text, re.M)
        if match is None:
            problems.append('%s（.env.example 里没有）' % name)
            continue
        actual = _normalize_default(match.group(1))
        if actual != expected:
            problems.append(
                '%s：.env.example 写 %s，config.py 是 %s' % (name, actual, expected))
    assert not problems, '；'.join(problems)


# --------------------------------------------------------------------------
# 6.1 防恒绿：合成违规样本，守卫必须抓到
# --------------------------------------------------------------------------
def test_guard_detects_env_var_missing_from_deploy_doc():
    """部署说明漏写一个变量 → 守卫必须报「缺失」。"""
    text = _read(DEPLOY_DOC_PATH)
    expected = config_env_vars()
    victim = sorted(expected)[0]
    broken = '\n'.join(
        line for line in text.splitlines() if victim not in line)
    missing, _extra = check_deploy_env_table(broken, expected)
    assert victim in missing, '守卫没抓到部署说明里漏写的变量 %s' % victim


def test_guard_detects_env_var_missing_from_env_example():
    """.env.example 漏掉一个变量 → 守卫必须报「缺失」。"""
    text = _read(ENV_EXAMPLE_PATH)
    expected = config_env_vars()
    victim = sorted(expected)[0]
    broken = '\n'.join(
        line for line in text.splitlines()
        if not line.startswith(victim + '='))
    missing = check_env_example_covers(broken, expected)
    assert victim in missing, '守卫没抓到 .env.example 里漏掉的变量 %s' % victim


def test_guard_detects_hardcoded_password_in_env_example():
    """.env.example 里写死口令 → 守卫必须抓到。"""
    text = _read(ENV_EXAMPLE_PATH)
    broken = text.replace('TPOPS_PASSWORD=\n', 'TPOPS_PASSWORD=Gauss@135\n', 1)
    assert broken != text, '合成样本没改成 —— .env.example 的格式变了？'
    problems = check_env_example_secrets(broken)
    assert 'TPOPS_PASSWORD' in problems, '守卫没抓到 .env.example 里的硬编码口令'


# --------------------------------------------------------------------------
# 5. 「文档 == 实现代码」—— wave-4 首轮审查的核心缺口
# --------------------------------------------------------------------------
#
# 上面那些守卫验的是「文档 == 生成脚本(映射表)」。
# 但 wave-4 首轮审查的 Critical C-1 正是从这条缝里溜过去的：
# 映射表里没有「A 类怎么归集」这条信息，生成器按自己的理解写，
# 守卫再拿生成器当真相源 → **文档错了、守卫还是绿的**。
#
# 下面这几条守卫的真相源是**生产代码**：
#   * A 类 / B 类的归集口径 → 与生产采集器的行为一致
#   * 「实际产出频率」→ 与生产调度代码算出的通道周期一致
#   * 排障表承诺的日志/退出码 → 生产代码里真的存在


def _production_channel_periods():
    """生产调度代码算出的通道周期（**不经过生成器**）。"""
    from tpops_kafka_collector.mapping.loader import load_mapping
    from tpops_kafka_collector.schedule import compute_channel_plans
    mapping = load_mapping()
    return dict(
        (plan.channel, plan.interval_seconds)
        for plan in compute_channel_plans(mapping)
    )


def test_a_class_collection_semantics_match_the_implementation(rules):
    """A 类（监控指标接口）是「**逐组件各出一条**」，不是「每个 master 各出一条」。

    wave-4 首轮审查的 **Critical C-1**：生成器只按 ``node_scoped`` 分支，
    把 B 类的 master 口径套到了 A 类。实测 A 类是每条 series 一条消息
    （``Standby_status`` 出 9 条 = 9 个 DN，不是 3 个 master）——
    文档给客户的承诺与实现相反。
    """
    rows = builder.parse_metric_spec_rows(_read(SPEC_PATH))
    a_rows = [r for r in rows if r.get('取数通道') == '监控指标接口']
    assert a_rows, '口径说明里没有 A 类（监控指标接口）行 —— 守卫空转了？'
    for row in a_rows:
        code = (row.get('客户指标英文名') or '').strip('`')
        text = row.get('归集口径') or ''
        assert '组件' in text, (
            'A 类指标的归集口径没提「组件」：%s —— 实现是每条 series 各出一条'
            % code
        )
        assert '每个 master 各出一条' not in text, (
            'A 类指标被写成「每个 master 各出一条」：%s —— 与实现相反'
            % code
        )


def test_b_class_node_scoped_says_per_master(rules):
    """B 类 ``node_scoped=true`` 的必须是「每个 master 各出一条」。

    B 类与 A 类相反：合并 SQL 在每个 master 各跑一次，各 master 各自一条
    （不合并、不求和）—— ``node_scoped=false`` 的只取第一个 master CN 的一条。
    """
    by_code = dict((rule['metric_code'], rule) for rule in rules)
    rows = builder.parse_metric_spec_rows(_read(SPEC_PATH))
    checked = 0
    for row in rows:
        code = (row.get('客户指标英文名') or '').strip('`')
        rule = by_code.get(code)
        if rule is None or not rule.get('node_scoped'):
            continue
        if (rule.get('source') or {}).get('source') != 'command_center_sql':
            continue
        text = row.get('归集口径') or ''
        assert '每个 master 各出一条' in text, (
            'B 类 node_scoped 指标的归集口径没写「每个 master 各出一条」：%s' % code
        )
        checked += 1
    assert checked, '一条 B 类 node_scoped 指标都没核到 —— 守卫空转了？'


def test_actual_output_frequency_matches_the_production_schedule():
    """「实际产出频率」必须等于**生产调度代码**算出的通道周期。

    声明周期（客户指标库的 ``collect_cycle``）**不是**产出闸门 ——
    实现按通道到期执行，通道周期 = 该通道内最高需求档（最短周期）。
    文档若把声明周期当成产出频率，就是给客户错误承诺（wave-4 首轮 I-1）。
    """
    periods = _production_channel_periods()
    from tpops_kafka_collector.schedule import SOURCE_TO_CHANNEL
    rows = builder.parse_metric_spec_rows(_read(SPEC_PATH))
    checked = 0
    for row in rows:
        code = (row.get('客户指标英文名') or '').strip('`')
        channel = row.get('取数通道')
        seconds = periods.get(_channel_key(channel))
        if seconds is None:
            continue
        assert _to_seconds(row.get('实际产出频率') or '') == seconds, (
            '指标 %s 的「实际产出频率」与生产调度代码算出的通道周期（%s 秒）不一致'
            % (code, seconds)
        )
        checked += 1
    assert checked, '一条都没核到 —— 守卫空转了？'


def _channel_key(label):
    """文档里的「取数通道」标签 → 生产代码的通道名。"""
    return {
        '监控指标接口': 'monitor',
        '命令中心SQL': 'command_center',
        '诊断优化接口': 'diagnostic',
        '实例接口': 'instance',
    }.get(label)


def _to_seconds(text):
    """「1 分钟」/「30 秒」/「1 小时」→ 秒。"""
    text = (text or '').strip()
    match = re.match(r'^(\d+)\s*(秒|分钟|小时)$', text)
    if not match:
        return None
    value = int(match.group(1))
    unit = match.group(2)
    if unit == '秒':
        return value
    if unit == '分钟':
        return value * 60
    return value * 3600


def test_troubleshooting_table_only_quotes_existing_log_messages():
    """排障表引用的日志文案**必须在生产代码里真的存在**。

    wave-4 首轮审查 I-4：排障表引用过一条代码里**根本不存在**的日志
    （「重试后成功」）—— 照着它去日志里搜永远搜不到。

    日志文案带占位符（``%s`` / ``%d`` / 文档里的 ``XXX`` / ``N`` / ``……``），
    所以不按整句比对，而是取**连续 ≥6 字的中文片段**，要求它出现在
    生产代码的字符串常量集合里 —— 片段太短（无法唯一定位）就跳过。
    """
    text = _read(DEPLOY_DOC_PATH)
    quoted = re.findall(r'日志(?:报|有)?[「]([^」]{6,})[」]', text)
    assert quoted, '排障表里一条日志引用都没解析到 —— 表格格式变了？'
    universe = _production_string_literals()
    unverifiable = []
    missing = []
    for quote in quoted:
        runs = _cjk_runs(quote)
        longest = max(runs, key=len) if runs else ''
        if len(longest) < 6:
            unverifiable.append(quote)
            continue
        if longest not in universe:
            missing.append('%s（片段：%s）' % (quote, longest))
    assert not missing, (
        '排障表引用了生产代码里不存在的日志文案：%s' % '；'.join(missing)
    )


def _cjk_runs(text):
    """文本里连续的中文片段（≥2 字）。"""
    return re.findall(r'[\u4e00-\u9fff]{2,}', text or '')


def _production_string_literals():
    """生产包里全部字符串常量的拼接（用来核对日志文案）。"""
    import ast
    chunks = []
    root = os.path.join(REPO_ROOT, 'tpops_kafka_collector')
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d != '__pycache__']
        for name in filenames:
            if not name.endswith('.py'):
                continue
            path = os.path.join(dirpath, name)
            with open(path, 'r', encoding='utf-8') as handle:
                tree = ast.parse(handle.read(), filename=path)
            for node in ast.walk(tree):
                # py3.7 的 AST 里字符串是 ``ast.Str``（统一到 ``ast.Constant``
                # 是 3.8 才做的）—— 两种都得认，否则守卫在目标解释器上等于空转
                if isinstance(node, ast.Str):
                    chunks.append(node.s)
                elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                    chunks.append(node.value)
    return '\n'.join(chunks)


def _production_source_text():
    """生产包全部源码的文本（用来核对日志文案 / 退出码）。"""
    chunks = []
    root = os.path.join(REPO_ROOT, 'tpops_kafka_collector')
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d != '__pycache__']
        for name in filenames:
            if name.endswith('.py'):
                with open(os.path.join(dirpath, name), 'r', encoding='utf-8') as h:
                    chunks.append(h.read())
    return '\n'.join(chunks)


def test_unconsumed_env_vars_are_disclosed_as_inert():
    """**没被生产代码消费**的配置项，部署说明必须披露「改了也不生效」。

    wave-4 首轮审查 I-5：``KAFKA_TOPIC_PARTITIONS`` 是第三个空转变量
    （``cmd_limit`` / ``retry_max_attempts`` 之外），文档却写得像它生效，
    还承诺了代码里根本不存在的「分区数不符」报错。

    客户改一个「看起来能改、实际改了没用」的环境变量，是**运维层面的坑** ——
    必须如实说明。
    """
    expected = config_env_vars()
    unconsumed = _unconsumed_config_keys(expected)
    # **「读了但没有行为效果」**的一类：值确实被读出来传给了库，但那个参数
    # 在当前用法下不起作用 —— 自动判据看不出来，只能显式登记。
    #   KAFKA_TOPIC_PARTITIONS → 传给 KafkaProducer(partitions=...)，但采集端
    #   **不创建 Topic、也不校验分区数**，实际分区数由 broker 决定 → 改了没用。
    unconsumed = set(unconsumed) | set(KNOWN_INERT_ENV_VARS)
    assert unconsumed, '没有空转变量了？—— 若已全部接上，把这条守卫删掉'
    text = _read(DEPLOY_DOC_PATH)
    for name in sorted(unconsumed):
        assert name in text, '配置项 %s 没被生产代码消费，部署说明里却没提' % name
        # 提到它的行可能有好几处（前置条件表、变量表…），
        # 只要**其中一处**写明了「改了也不生效」就算披露到位
        lines = [l for l in text.splitlines() if name in l]
        inert_words = ('未消费', '未生效', '没有影响', '尚未被消费', '未被消费')
        assert any(
            any(word in line for word in inert_words) for line in lines
        ), (
            '配置项 %s 没被生产代码消费，但部署说明没说明「改了也不生效」：%s'
            % (name, ' / '.join(l.strip()[:100] for l in lines[:2]))
        )


def _unconsumed_config_keys(expected):
    """config.py 产出的配置项里，**生产代码根本不消费**的那些（返回环境变量名）。

    判据：配置项的**键名**（``alarm_limit``）在生产包源码里（除 config.py 自身）
    一次都没出现 → 没有任何代码读它 → 改了也不生效。

    文档与 ``.env.example`` 用的是**环境变量名**（``ALARM_LIMIT``），
    所以返回环境变量名，便于在文档里逐个核对披露。
    """
    import ast
    config_path = os.path.join(REPO_ROOT, 'tpops_kafka_collector', 'config.py')
    with open(config_path, 'r', encoding='utf-8') as handle:
        config_source = handle.read()
    tree = ast.parse(config_source, filename=config_path)
    pairs = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        for key, value in zip(node.keys, node.values):
            if not isinstance(key, ast.Str) or not isinstance(value, ast.Call):
                continue
            if not value.args:
                continue
            env_arg = value.args[1] if len(value.args) > 1 else None
            if isinstance(env_arg, ast.Str) and env_arg.s.isupper():
                pairs.append((key.s, env_arg.s))
    # 判据：**键名出现在哪些函数里**，这些函数**有没有被调用过**。
    # 只看「名字出现过」会把「定义在没人调用的函数里」误判成已消费 ——
    # 第二轮复审就是这么发现 KAFKA_TOPIC_PARTITIONS 漏网的
    # （producer.ensure_topics() 里用了，但它**没有生产调用方**）。
    defined, called, usage = _production_function_usage()
    unconsumed = set()
    for key, env_name in pairs:
        holders = usage.get(key) or set()
        if not holders:
            unconsumed.add(env_name)
            continue
        if any(name not in defined or name in called for name in holders):
            continue          # 至少有一处是在**被调用**的函数里用 → 算消费
        unconsumed.add(env_name)
    return unconsumed


def _production_function_usage():
    """生产包里的函数定义、被调用的函数名，以及「配置键名 → 用到它的函数名」。"""
    import ast
    root = os.path.join(REPO_ROOT, 'tpops_kafka_collector')
    defined = set()
    called = set()
    usage = {}
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d != '__pycache__']
        for name in filenames:
            if not name.endswith('.py'):
                continue
            path = os.path.join(dirpath, name)
            with open(path, 'r', encoding='utf-8') as handle:
                source = handle.read()
            tree = ast.parse(source, filename=path)
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    defined.add(node.name)
                    body = ast.dump(node)
                    for key in re.findall(r"'([a-z][a-z0-9_]{3,})'", body):
                        usage.setdefault(key, set()).add(node.name)
                elif isinstance(node, ast.Call):
                    func = node.func
                    if isinstance(func, ast.Name):
                        called.add(func.id)
                    elif isinstance(func, ast.Attribute):
                        called.add(func.attr)
    return defined, called, usage


# --------------------------------------------------------------------------
# 6. 第二轮复审指出的 7 条 —— 全是「守卫守不住」的洞
# --------------------------------------------------------------------------
#
# wave-4 第二轮复审结论：9 条**文档**发现已全部闭环（复审方自己重放生产流水线验过），
# 但**守卫守不住** —— 首轮 6 条恒绿变异里 5 条仍绿，另有两处新守卫有洞。
#
# 下面这批守卫的共同点：**判据从「关键词」升级为「语义 / 真相源」**。
#   关键词判断（「文本里有没有『组件』两个字」）挡不住语义错误
#   （「所有组件求和后出一条」照样含「组件」）。

#: A 类归集口径里**禁止**出现的表述（这些是语义错误的说法）
_FORBIDDEN_A_COLLECTION = (
    '合成一条', '求和后', '合并成一条', '汇总为一条', '聚合成一条',
    '求和出一条', '合并后一条',
)

#: **「读了但没有行为效果」**的环境变量（自动判据看不出来，显式登记）。
#: 见 ``test_unconsumed_env_vars_are_disclosed_as_inert`` 的说明。
KNOWN_INERT_ENV_VARS = ('KAFKA_TOPIC_PARTITIONS',)


def test_a_class_collection_forbids_synthetic_aggregation():
    """A 类口径不得出现「合成/求和后出一条」这类**语义错误**的说法。

    第二轮复审：上一版守卫只查「含不含『组件』」「含不含『每个 master 各出一条』」，
    所以「组件级：所有组件求和后出一条」这种**完全相反**的说法能通过。
    """
    rows = builder.parse_metric_spec_rows(_read(SPEC_PATH))
    a_rows = [r for r in rows if r.get('取数通道') == '监控指标接口']
    assert a_rows, '口径说明里没有 A 类行 —— 守卫空转了？'
    for row in a_rows:
        code = (row.get('客户指标英文名') or '').strip('`')
        text = row.get('归集口径') or ''
        for bad in _FORBIDDEN_A_COLLECTION:
            assert bad not in text, (
                'A 类指标 %s 的归集口径出现了语义错误的说法「%s」：%s'
                % (code, bad, text)
            )


def test_troubleshooting_log_quotes_check_every_fragment():
    """排障表引用的日志：**每一个** ≥4 字的中文片段都必须在生产代码里存在。

    第二轮复审：上一版只取**最长**的中文片段，<6 字的直接跳过 →
    「命令中心 …… 重试后成功」（最长片段 5 字）能通过。
    现在改成**逐片段**检查，阈值降到 4 字。
    """
    text = _read(DEPLOY_DOC_PATH)
    quoted = re.findall(r'日志(?:报|有)?[「]([^」]{6,})[」]', text)
    assert quoted, '排障表里一条日志引用都没解析到 —— 表格格式变了？'
    universe = _production_string_literals()
    missing = []
    for quote in quoted:
        for run in set(_cjk_runs(quote)):
            if len(run) < 4:
                continue
            if run not in universe:
                missing.append('%s（片段：%s）' % (quote, run))
    assert not missing, (
        '排障表引用了生产代码里不存在的日志片段：%s' % '；'.join(missing)
    )


def test_deploy_required_flags_match_the_config_source():
    """部署说明的「必填」列必须与 ``config.py`` 的**必填判定**一致。

    第二轮复审 M3：把 ``TPOPS_PASSWORD`` 的必填从「是」改成「否」，守卫全绿。
    """
    from tpops_kafka_collector.config import ConfigError  # noqa: F401
    import tpops_kafka_collector.config as config_mod
    required = _required_env_vars()
    rows = _deploy_env_rows()
    for row in rows:
        name = (row.get('变量') or '').strip('`')
        if not name:
            continue
        flag = (row.get('必填') or '').strip().replace('*', '').strip()
        if name in required:
            assert flag == '是', (
                '环境变量 %s 在 config.py 里是必填（缺失即抛 ConfigError），'
                '但部署说明写的是「%s」' % (name, flag)
            )
        else:
            assert flag != '是', (
                '环境变量 %s 在 config.py 里不是必填，部署说明却写「是」' % name
            )
    del config_mod


def _required_env_vars():
    """config.py 里**缺失即报错**的环境变量名（``_require`` / ``_servers``）。"""
    import ast
    config_path = os.path.join(REPO_ROOT, 'tpops_kafka_collector', 'config.py')
    with open(config_path, 'r', encoding='utf-8') as handle:
        tree = ast.parse(handle.read(), filename=config_path)
    required = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
            continue
        if node.func.id not in ('_require', '_servers'):
            continue
        for arg in node.args[1:2]:
            if isinstance(arg, ast.Str):
                required.add(arg.s)
    return required


def _deploy_env_rows():
    """部署说明里的环境变量表行。"""
    rows = []
    for line in _read(DEPLOY_DOC_PATH).splitlines():
        if not line.startswith('|'):
            continue
        cells = [c.strip() for c in line.strip('|').split('|')]
        if len(cells) < 4 or not cells[0].startswith('`'):
            continue
        rows.append(dict(zip(('变量', '默认值', '必填', '说明', '改错的后果'), cells)))
    return rows


def test_deploy_exit_codes_exist_in_the_production_code():
    """部署说明提到的退出码必须真实存在（``EXIT_*`` 常量）。

    第二轮复审 M4：后果文案写错（比如退出码写成 4）没人发现。
    """
    from tpops_kafka_collector import main as main_mod
    real = set(
        value for name, value in vars(main_mod).items()
        if name.startswith('EXIT_') and isinstance(value, int)
    )
    assert real, '生产代码里没有 EXIT_* 常量了？—— 若改名，把这条守卫一起改'
    text = _read(DEPLOY_DOC_PATH)
    # 文档里退出码常写成**加粗**的（``退出码 **2**``）—— 先去掉 markdown 强调标记
    # 再取数字，否则最显眼的那几处声明根本匹配不到（第二轮复审 M4 的半个洞）
    plain = text.replace('*', '')
    quoted_codes = set(int(m) for m in re.findall(r'退出码\s*(\d+)', plain))
    assert quoted_codes, '部署说明里一个退出码都没提 —— 格式变了？'
    unknown = quoted_codes - real
    assert not unknown, (
        '部署说明提到了代码里不存在的退出码：%s（真实退出码：%s）'
        % ('、'.join(str(c) for c in sorted(unknown)), sorted(real))
    )


def test_deploy_required_sections_are_present():
    """部署说明的**必备小节**不能被删掉（标题在 **且** 正文非空）。

    第二轮复审 M5 的三个叠加洞：
      (a) 只做标题子串匹配；
      (b) ``startswith('#')`` 把 ``` 围栏里的 **shell 注释**（``# 1) 拷贝产物``）
          当成章节标题 —— 实测删掉整节后守卫报的却是别的章节缺失；
      (c) **只查标题不查正文** —— 删掉整节「运行期文件」仍全绿。
    现在：先剥掉代码围栏，再要求「标题存在 **且** 该节正文非空」。
    """
    text = _read(DEPLOY_DOC_PATH)
    stripped = _strip_code_fences(text)
    lines = stripped.splitlines()
    headings = [
        (index, line.lstrip('#').strip())
        for index, line in enumerate(lines)
        if line.startswith('#')
    ]
    required_sections = (
        '环境变量', '部署步骤', '启动', '优雅退出', '运行期文件',
        '健康检查', '日志', '排障',
    )
    problems = []
    for name in required_sections:
        matched = [
            index for index, heading in headings if name in heading
        ]
        if not matched:
            problems.append('%s（缺这一节）' % name)
            continue
        # 该节的正文：从标题行到下一个标题行之间，必须有内容
        bodies = []
        for index in matched:
            end = len(lines)
            for other, _ in headings:
                if other > index:
                    end = other
                    break
            body = [
                l for l in lines[index + 1:end]
                if l.strip() and not l.startswith('#')
            ]
            bodies.append(len(body))
        if not any(count > 0 for count in bodies):
            problems.append('%s（标题在但正文是空的）' % name)
    assert not problems, '部署说明的必备小节有问题：%s' % '、'.join(problems)


def _strip_code_fences(text):
    """去掉 ``` 围起来的代码块（里面的 ``#`` 注释不是章节标题）。"""
    out = []
    inside = False
    for line in text.splitlines():
        if line.strip().startswith('```'):
            inside = not inside
            continue
        if not inside:
            out.append(line)
    return '\n'.join(out)


def test_value_rule_column_matches_the_mapping_table():
    """口径说明的「取值规则」列必须等于**从映射表重新推导**的文本。

    第二轮复审 M2：把映射表里的 value_rule 改了（真相源变了），
    文档没跟着变，守卫全绿。

    判据不是「文档 == 生成器输出」（那只是自证），而是
    **「文档 == 用当前映射表重新推导出的文本」** —— 真相源一变，
    重新推导的结果就变，文档不跟着变就红。
    """
    rules = json.load(open(METRIC_RULES_PATH, 'r', encoding='utf-8'))['rules']
    by_code = dict((r['metric_code'], r) for r in rules)
    rows = builder.parse_metric_spec_rows(_read(SPEC_PATH))
    checked = 0
    for row in rows:
        code = (row.get('客户指标英文名') or '').strip('`')
        rule = by_code.get(code)
        if rule is None or not rule.get('value_rule'):
            continue
        expected = builder.describe_value_rule(rule.get('value_rule'))
        if not expected:
            continue
        text = (row.get('取值规则') or '').strip()
        assert text == expected.strip(), (
            '指标 %s 的取值规则列与映射表重新推导的结果不一致：\n'
            '  文档：%s\n  推导：%s' % (code, text, expected)
        )
        checked += 1
    assert checked, '一条都没核到 —— 守卫空转了？'


def _rule_type_alias(rule_type):
    """value_rule.type 的中文别名（文档里用的是人话）。"""
    return {
        'series_point': '时序',
        'sum_column': '求和',
        'delta_rate': '差分',
        'counter_diff': '差分',
    }.get(rule_type, rule_type)
