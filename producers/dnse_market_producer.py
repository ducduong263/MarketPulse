import asyncio
import os
from dotenv import load_dotenv
from trading_websocket import TradingClient
from trading_websocket.models import Trade

load_dotenv()

async def main():
    encoding = "json" 
    
    client = TradingClient(
        api_key=os.getenv("DNSE_API_KEY"),
        api_secret=os.getenv("DNSE_API_SECRET"),
        base_url="wss://ws-openapi.dnse.com.vn",
        encoding=encoding,
    )

    def handle_trade(trade: Trade): 
        data_to_print = trade.dict() if hasattr(trade, "dict") else trade
        print(f">>> [TICK RECEIVED] Dữ liệu trả về:\n{data_to_print}")

    print(f"[START] Đang kết nối tới wss://ws-openapi.dnse.com.vn...")
    await client.connect()
    print("[SUCCESS] Đã kết nối thành công! Đang chờ dữ liệu đổ về...")

    print("[INIT] Đang đăng ký topic với máy chủ...")
    await client.subscribe_trades(
        symbols=["FPT", "VIC", "SSI", "HPG", "MWG"],
        on_trade=handle_trade, 
        encoding=encoding
    )
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\n[STOP] Đã ngắt kết nối.")

if __name__ == "__main__":
    asyncio.run(main())