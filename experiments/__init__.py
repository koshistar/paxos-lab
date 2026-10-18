"""实验编排包：矩阵跑批、数据分析、画图。

* ``run_matrix.py`` —— 一键跑完整个实验矩阵，把每条结果写进 ``results/summary.csv``
* ``analyze.py``    —— 汇总成报告要用的表格（Markdown / CSV）
* ``plots.py``      —— 生成报告用的 PNG 图

这三者都只依赖标准库；``plots.py`` 需要 matplotlib，``analyze.py`` 用纯标准库实现
（这样即使没装任何第三方包，也能出表格）。
"""
