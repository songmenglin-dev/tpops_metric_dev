# 进度账本 —— 变更 tpops-kafka-collector

计划：sdd，revision 2（6 个 wave，依赖链 wave-0 → wave-1 → wave-2a → wave-2b → wave-3 → wave-4）
工作树：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`，分支 `tpops_metric_dev`

## Wave 0 —— wave-0-mapping-baseline（serial）

- [ ] 0.1 建包并复制映射层
- [ ] 0.2 在包内加 merged_views 段与合并形态解码
- [ ] 0.3 在包内修告警幂等键
- [ ] 0.4 在包内修跨视图比率
- [ ] 0.5 合并形态的列校验与证据重推导
- [ ] 0.6 同步开发方案的契约章节
- [ ] 0.7 把依赖切到包内并删除参考副本

## Wave 1 —— wave-1-skeleton-platform（parallel）

- [ ] 1.1~1.9（配置 / DTO+异常 / 重试 / 序列化 / 生产者 / 客户端 / 映射表加载 / 规则引擎 / 水位）

## Wave 2a —— wave-2a-mapping-data（serial）

- [ ] 2.1 映射数据复制进包
- [ ] 2.2 合并 SQL 的运行时渲染

## Wave 2b —— wave-2b-channels（parallel）

- [ ] 2.3~2.10（A 类 / 命令中心 / B 类 / C 类 / D 类 / E 类 / 脱敏）

## Wave 3 —— wave-3-scheduling-testing（serial）

- [ ] 3.1~3.5（调度器 / 健康端点 / 入口 / 回归夹具 / 门禁）

## Wave 4 —— wave-4-delivery-docs（serial）

- [ ] 4.1 交付文档
