# -*- coding: utf-8 -*-
"""交付文档守卫：文档必须与真相源（映射表 / 客户指标表）一致。

覆盖三份客户交付文档：

  1. ``docs/交付-指标口径说明.md``          —— 覆盖全部 98 个可提供指标
  2. ``docs/交付-不可提供指标清单.md``      —— 恰好 3 条，与 ``enabled=false`` 完全一致
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
REQUIRED_SPEC_COLUMNS = ('客户指标英文名', '中文名', '取数通道', '来源',
                         '取值规则', '刷新周期', '资源粒度', '归集口径')

#: 像指标名的串（用于「文档里不能出现不存在的指标」）
METRIC_CODE_RE = re.compile(r'`((?:gaussdb|GAUSSDB)[^`]*)`')


@pytest.fixture(scope='module')
def rules():
    """映射表的 101 条规则。"""
    with open(METRIC_RULES_PATH, 'r', encoding='utf-8') as handle:
        return json.load(handle)['rules']


@pytest.fixture(scope='module')
def enabled_codes(rules):
    """98 个可提供指标的 metric_code。"""
    return set(rule['metric_code'] for rule in rules if rule.get('enabled'))


@pytest.fixture(scope='module')
def disabled_codes(rules):
    """3 个不可提供指标的 metric_code。"""
    return set(rule['metric_code'] for rule in rules if not rule.get('enabled'))


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
    assert len(enabled_codes) == 98, '可提供指标数不是 98，映射表变了？'


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
def test_unsupported_doc_lists_exactly_disabled_metrics(disabled_codes):
    """不可提供清单 = 3 条，且与 metric_rules.json 里 enabled=false 的完全一致。"""
    text = _read(UNSUPPORTED_PATH)
    missing, extra = check_unsupported_list(text, disabled_codes)
    assert not missing, '不可提供清单缺少：%s' % '、'.join(missing)
    assert not extra, '不可提供清单多出：%s' % '、'.join(extra)
    assert len(disabled_codes) == 3, '不可提供指标数不是 3，映射表变了？'


def test_unsupported_doc_explains_reason_and_workaround(rules):
    """每条都要有「为什么给不了」和「能不能变通」，且理由来自映射表。"""
    text = _read(UNSUPPORTED_PATH)
    for rule in rules:
        if rule.get('enabled'):
            continue
        code = rule['metric_code']
        assert code in text, '不可提供清单里没有 %s' % code
        reason = rule.get('unsupported_reason') or rule.get('note') or ''
        assert reason and reason in text, \
            '不可提供清单没有引用映射表里的理由：%s' % code
        assert '**能不能变通**' in text, '不可提供清单没有写变通方案'


def test_unsupported_doc_states_drop_count(rules):
    """要写明这 3 条在采集时被丢弃并计数。"""
    text = _read(UNSUPPORTED_PATH)
    assert 'dropped_unsupported' in text
    assert '丢弃并计数' in text
    assert '= **3**' in text or '= 3' in text


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


def test_detector_finds_known_ocr_typos(rules):
    """自动检测器要能抓到已知的 OCR 错字。"""
    findings = builder.detect_ocr_findings(rules)
    pairs = set((item['metric_code'], item['token']) for item in findings)
    assert ('GAUSSDB.Number_of_ansactions_mmitted_r_cond', 'ansactions') in pairs
    assert ('GAUSSDB.Number_of_ansactions_mmitted_r_cond', 'mmitted') in pairs
    assert ('GAUSSDB.Number_of_llback_ansactions_r_cond', 'llback') in pairs
    assert ('gaussdb.{#NAME}.number.Of.dex.ples.ad', 'dex') in pairs
    assert ('GAUSSDB.Checkpoint_count_eckpoint_gments', 'gments') in pairs


def test_ocr_review_entries_are_corroborated(rules):
    """人工复核表里的每条，检测器都要能给出候选（复核表不能凭空调）"""
    findings = builder.detect_ocr_findings(rules)
    candidates = {}
    for item in findings:
        candidates.setdefault(item['metric_code'], set()).add(item['token'])
    for code, items in builder.OCR_REVIEW.items():
        for bad, _good, _why in items:
            for token in builder.code_tokens(bad):
                assert token in candidates.get(code, set()), \
                    '复核表里的 %s / %s 没有检测器佐证' % (code, bad)


def test_other_docs_only_mention_known_metrics(rules):
    """不可提供清单与 OCR 清单里出现的指标名也必须存在于映射表。"""
    known = set(rule['metric_code'] for rule in rules)
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


def test_guard_detects_wrong_unsupported_list(disabled_codes):
    """不可提供清单里塞进一条可提供指标 → 守卫必须报「多余」。"""
    text = _read(UNSUPPORTED_PATH)
    broken = _append_table_row(
        text, builder.TABLE_MARKS['unsupported'],
        '| 999 | `gaussdb.Tps` | 假不可提供 | 假的 | 假的 | 假的 |')
    missing, extra = check_unsupported_list(broken, disabled_codes)
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

#: 凭据类变量 —— .env.example 里必须留空（禁硬编码口令）。
SECRET_ENV_VARS = ('TPOPS_PASSWORD',)

#: config.py 应当读取的环境变量数（清单见开发方案 §5.5）。
EXPECTED_ENV_VAR_COUNT = 30


def config_env_vars():
    """config.py 实际读取的环境变量名集合。"""
    return set(re.findall(
        r"env,\s*'([A-Z][A-Z0-9_]+)'", _read(CONFIG_PATH)))


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
