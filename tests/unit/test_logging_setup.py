"""日志中心化初始化的**元测试**（任务 3.3）。

守的是「配置真的生效」这件事 ——
``config.py`` 里的 ``log_level`` / ``log_format`` 早就有定义，
但在本模块之前**没有任何代码消费它们**（定义了、写了、没人调用）。
这里的每个用例都必须真的看到输出，否则守卫失效。
"""
import io
import json
import logging

from tpops_kafka_collector.logging_setup import setup_logging


def _stream(level='INFO', log_format='text'):
    """装一次日志，把输出接到内存流上。"""
    stream = io.StringIO()
    setup_logging(level, log_format, stream=stream)
    return stream


def _records(stream):
    return [line for line in stream.getvalue().splitlines() if line.strip()]


def test_debug_level_really_outputs_info_records():
    """配 DEBUG → INFO 日志真的能输出（不是被 lastResort 吞掉）。"""
    stream = _stream('DEBUG', 'text')
    logging.getLogger('tpops_kafka_collector.demo').info('hello 世界')
    output = stream.getvalue()
    assert 'hello 世界' in output
    assert 'INFO' in output


def test_info_level_is_silenced_when_level_is_warning():
    """配 WARNING → INFO 日志不输出（级别真的被消费了）。"""
    stream = _stream('WARNING', 'text')
    logging.getLogger('tpops_kafka_collector.demo').info('不该出现')
    assert '不该出现' not in stream.getvalue()


def test_text_format_is_time_level_module_message():
    """text 格式：loguru 原生形态 —— 时间 | 级别 | 模块:函数:行号 - 消息。

    wave-2 起 text 用的是 loguru 原生格式（不再是自研的 logback pattern），
    所以这里断言的是「原生形态」：三段、级别单独一段、末尾是消息。
    """
    stream = _stream('INFO', 'text')
    logging.getLogger('tpops_kafka_collector.demo').info('一条消息')
    line = _records(stream)[0]
    parts = [part.strip() for part in line.split('|')]
    assert len(parts) == 3, line
    assert parts[1] == 'INFO'
    assert 'test_logging_setup' in parts[2], line
    assert parts[2].endswith('一条消息'), line


def test_json_format_emits_one_json_object_per_line():
    """配 JSON → 真的逐行 json.loads 得通。"""
    stream = _stream('INFO', 'json')
    logger = logging.getLogger('tpops_kafka_collector.demo')
    logger.info('第一条')
    logger.warning('第二条')
    lines = _records(stream)
    assert len(lines) == 2, lines
    payloads = [json.loads(line) for line in lines]
    assert payloads[0]['message'] == '第一条'
    assert payloads[0]['level'] == 'INFO'
    assert payloads[0]['logger'] == 'tpops_kafka_collector.demo'
    assert payloads[1]['level'] == 'WARNING'


def test_json_format_flattens_key_value_summary_into_fields():
    """每轮汇总日志的 key=value 在 JSON 格式下成为顶层字段。

    字段名由 ``scheduler_report.py`` 定死（tick/elapsed/fetched/…），
    JSON 格式要能直接取到，而不是只塞在 message 字符串里。
    """
    stream = _stream('INFO', 'json')
    logging.getLogger('tpops_kafka_collector.scheduler_report').info(
        'tick=7 elapsed=14.8s fetched=1043 mapped=1041 produced=1041 '
        'failed=0 dropped_unsupported=3 windows=9 alarms=2 calls=23 '
        'delivery_rate=1.000 groups=monitor skipped=-'
    )
    payload = json.loads(_records(stream)[0])
    assert payload['tick'] == '7'
    assert payload['elapsed'] == '14.8s'
    assert payload['fetched'] == '1043'
    assert payload['dropped_unsupported'] == '3'
    assert payload['delivery_rate'] == '1.000'


def test_setup_logging_is_idempotent():
    """重复调用不叠加 handler（basicConfig(force=…) 是 3.8+，py37 自己判）。"""
    root = logging.getLogger()
    before = len(root.handlers)
    setup_logging('INFO', 'text', stream=io.StringIO())
    setup_logging('INFO', 'text', stream=io.StringIO())
    assert len(root.handlers) == before


def test_setup_logging_keeps_single_owned_handler():
    """本模块装上去的 handler 永远只有一个。"""
    setup_logging('INFO', 'text', stream=io.StringIO())
    setup_logging('INFO', 'json', stream=io.StringIO())
    root = logging.getLogger()
    owned = [
        handler for handler in root.handlers
        if getattr(handler, '_tpops_collector_handler', False)
    ]
    assert len(owned) == 1


def test_noisy_third_party_loggers_are_quieted():
    """kafka / urllib3 等第三方库被压到 WARNING。"""
    setup_logging('DEBUG', 'text', stream=io.StringIO())
    assert logging.getLogger('kafka').level == logging.WARNING
    assert logging.getLogger('urllib3').level == logging.WARNING


def test_output_goes_to_stderr_by_default(capsys):
    """默认输出到 stderr（常驻进程的标准做法）。

    wave-2 起根 handler 是**转发器**（把 stdlib 日志送进 loguru），
    真正写 stderr 的是 loguru 的 sink —— 所以这里断言「输出真的落到 stderr」。
    """
    setup_logging('INFO', 'text')
    root = logging.getLogger()
    owned = [
        handler for handler in root.handlers
        if getattr(handler, '_tpops_collector_handler', False)
    ]
    assert owned, '本模块应装了一个 handler'
    logging.getLogger('tpops_kafka_collector.demo').info('落到 stderr 了')
    assert '落到 stderr 了' in capsys.readouterr().err
