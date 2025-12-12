import qlib
from qlib.data import D


def main() -> None:
    # 在 WSL 环境中执行本脚本时，默认使用下面的 qlib_bin_20251209 路径
    # 如有不同挂载路径，可按需修改 provider_uri
    provider_uri = "/home/lc999/AIstock/qlib_bin/qlib_bin_20251209"

    qlib.init(provider_uri=provider_uri)

    stocks = D.list_instruments({"market": "all"})
    print("all size:", len(stocks))
    print("first 10:", stocks[:10])

    has_000300 = any(s.instrument == "000300.SH" for s in stocks)
    print("has 000300.SH in all:", has_000300)

    indexes = D.list_instruments({"market": "index"})
    print("index instruments:", indexes)


if __name__ == "__main__":
    main()
