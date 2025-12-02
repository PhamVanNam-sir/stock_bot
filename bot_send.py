import os
import warnings 
warnings.filterwarnings("ignore")
from vnstock import Vnstock, Quote, Listing
import pandas as pd
from datetime import datetime
import datetime as dt
from dateutil.relativedelta import relativedelta
import time
import requests
import pytz 

stock = Vnstock().stock(symbol='FPT', source='VCI')

# --- CẤU HÌNH DISCORD ---
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN")
DISCORD_CHANNEL_ID = os.environ.get("DISCORD_CHANNEL_ID")

VN_TZ = pytz.timezone('Asia/Ho_Chi_Minh')
time_sleep = 60
# List mã của bạn
full = [
    "AAA", "ACB", "AGR", "ANV", "ASM", "BAF", "BCG", "BID", "BSI", "BSR", "CII",
    "CRE", "CSM", "CTG", "CTI", "CTS", "DBC", "DCM", "DGC", "DGW", "DIG", "DLG",
    "DPG", "DPM", "DSE", "DXG", "DXS", "EIB", "EVF", "EVG", "FCN", "FPT", "FTS",
    "GEX", "GMD", "GVR", "HAG", "HAH", "HCM", "HDB", "HDC", "HDG", "HHS", "HHV",
    "HPG", "HPX", "HQC", "HSG", "HTN", "HVN", "IDI", "IJC", "KBC", "KDH", "KHG",
    "KSB", "LCG", "LDG", "LPB", "MBB", "MSB", "MSN", "MWG", "NAB", "NKG", "NLG",
    "NT2", "NTL", "NVL", "OCB", "ORS", "PAN", "PC1", "PDR", "PET", "PLX", "POW",
    "PVD", "PVT", "QCG", "SCR", "SHB", "SSB", "SSI", "STB", "SZC", "TCB", "TCH",
    "TCM", "TPB", "TTF", "VCB", "VCG", "VCI", "VDS", "VGC", "VHC", "VHM", "VIB",
    "VIC", "VIX", "VJC", "VND", "VNM", "VOS", "VPB", "VPI", "VRE", "VSC", "YEG"
]

vn100 = Listing().symbols_by_group('VN100').tolist()
cp = list(set(vn100) | set(full))

alert_tracker = {} 

# --- HÀM GỬI DISCORD (Dùng Requests) ---
def send_discord(message):
    if not DISCORD_TOKEN or not DISCORD_CHANNEL_ID:
        print("❌ Chưa cấu hình Token/Channel ID Discord!")
        return
    
    url = f"https://discord.com/api/v9/channels/{DISCORD_CHANNEL_ID}/messages"
    
    # Header xác thực bot
    headers = {
        "Authorization": f"Bot {DISCORD_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Payload tin nhắn
    payload = {
        "content": message
    }
    
    while True:
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=10)
            if response.status_code == 200: 
                break # Gửi thành công
            elif response.status_code == 429: 
                # Bị Discord chặn do spam nhanh quá -> Đợi xíu
                retry_after = response.json().get('retry_after', 5)
                print(f"⚠️ Rate limited. Đợi {retry_after}s...")
                time.sleep(retry_after)
            else: 
                print(f"Lỗi gửi tin: {response.status_code} - {response.text}")
                time.sleep(5)
        except Exception as e: 
            print(f"Lỗi kết nối: {e}")
            time.sleep(5)

# --- TRỊ TẬN GỐC LỖI SYSTEM EXIT CỦA VNSTOCK/VNAI ---
import sys
import time
import re
from vnai.beam.quota import CleanErrorContext

# 1. Định nghĩa hành vi mới: Khi lỗi xảy ra, KHÔNG ĐƯỢC tắt chương trình
# 1. Định nghĩa hành vi mới
def safe_exit_smart(self, exc_type, exc_val, exc_tb):
    if exc_type:
        error_msg = str(exc_val)
        print(f"\n🛡️ [ANTI-CRASH] Đã chặn lệnh tắt. Lỗi từ server: {error_msg}")
        
        # Dùng Regex để tìm con số giây trong thông báo lỗi
        # Ví dụ: "Vui lòng thử lại sau 49 giây" -> Tìm thấy số 49
        match = re.search(r'sau (\d+) giây', error_msg)
        
        wait_time = 60 # Mặc định ngủ 60s nếu không tìm thấy số
            
        print(f"🛑 Server yêu cầu chờ {match.group(1) if match else '???'}s.")
        print(f"💤 Bot sẽ ngủ {wait_time}s ngay lập tức để tuân thủ luật chơi...")
        
        # NGỦ NGAY TẠI ĐÂY - KHÔNG CHO CODE CHẠY TIẾP
        time.sleep(wait_time)
        
        print("⚡ Đã ngủ xong! Tiếp tục thử lại...")
        
        # Return False để báo cho Python biết là "tao đã xử lý xong phần ngủ rồi,
        # nhưng cứ ném lỗi ra ngoài để vòng lặp bên ngoài biết mà retry lại từ đầu"
        return False 
    return False

# 2. Tiêm thuốc
CleanErrorContext.__exit__ = safe_exit_smart

print("✅ Đã cập nhật ANTI-CRASH phiên bản Smart! (Tự động ngủ khi bị chặn)")

# --- 1. HÀM KHIÊN BẢO VỆ (CHỐNG SẬP) ---
def call_vnstock_safe(func, *args, **kwargs):
    """
    Hàm này bọc lấy lệnh lấy dữ liệu. 
    Nếu gặp lỗi SystemExit (do vnstock tự tắt) hoặc lỗi mạng, 
    nó sẽ chặn lại, không cho tắt, đợi 70s rồi thử lại.
    """
    while True:
        try:
            # Thử chạy lệnh lấy dữ liệu
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            # Cho phép bấm nút Stop (Interrupt) thủ công
            raise 
        except (Exception, SystemExit, BaseException) as e:
            # Bắt tất cả các thể loại lỗi, kể cả lệnh thoát chương trình
            err_msg = str(e)
            print(f"\n⚠️ Lỗi: {err_msg}")
            print("⏳ Đang bị chặn (Rate Limit). Ngủ 70s để 'nguội' máy...")
            time.sleep(60) 
            print("🔄 Đang thử lại...")

def notification(ticker, df_day, df_minute, price_threshold=None, vol_multiplier=1.5):
    # ==============================================================================
    # BƯỚC 0: KIỂM TRA QUOTA CẢNH BÁO (LOGIC MỚI CỦA SẾP)
    # ==============================================================================
    global alert_tracker
    
    # Khởi tạo tracker cho mã này nếu chưa có
    if ticker not in alert_tracker:
        alert_tracker[ticker] = {'AM': [], 'PM': []}
    
    # Xác định phiên hiện tại (Sau 13:00 là phiên chiều PM)
    current_hour = datetime.now().hour
    session = 'PM' if current_hour >= 13 else 'AM'
    
    # *** LOGIC TỐI ƯU ***: Nếu đã báo đủ 2 lần trong phiên -> DỪNG NGAY
    if len(alert_tracker[ticker][session]) >= 2:
        # print(f"-> {ticker} đã báo đủ 2 lần phiên {session}. Bỏ qua để tiết kiệm tài nguyên.")
        return 

    # ==============================================================================
    # BƯỚC 1: KHỞI TẠO BASELINE (GIỮ NGUYÊN)
    # ==============================================================================  
    try:
        df_day_ticker = df_day.loc[df_day['ticker'] == ticker]
        df_minute_ticker = df_minute.loc[df_minute['ticker'] == ticker].copy() # Copy để tránh warning
        
        if df_minute_ticker.empty:
            return

        df_minute_ticker.reset_index(inplace=True)
        
        if price_threshold is None:
            # Lấy giá trị close dòng cuối cùng
            if not df_day_ticker.empty:
                price_threshold = round(float(df_day_ticker['Close'].rolling(20).mean().iloc[-1]), 2)
            else:
                price_threshold = 0 # Fallback nếu không có data day

        # ==============================================================================
        # BƯỚC 1.5: LẤY DATA INTRADAY (CHỈ LẤY KHI CHƯA HẾT QUOTA)
        # ==============================================================================
        def get_intraday_task():
            return Quote(symbol=ticker, source='VCI').intraday(symbol=ticker, page_size=100_000)
        
        df_intraday = call_vnstock_safe(get_intraday_task)
        
        if df_intraday is None or df_intraday.empty:
            return

        # --- Xử lý data Minute History (Baseline) ---
        df_minute_ticker['Time'] = pd.to_datetime(df_minute_ticker['Time'])
        df_minute_ticker['date'] = df_minute_ticker['Time'].dt.date
        
        unique_dates = sorted(df_minute_ticker['date'].unique())
        recent_dates = unique_dates[-20:]
        
        daily_filled_data = []
        standard_time_range = pd.date_range(start="09:15", end="14:30", freq="1min").time
        
        for d in recent_dates:
            day_df = df_minute_ticker[df_minute_ticker['date'] == d].copy()
            day_df = day_df.set_index('Time')
            full_index = [datetime.combine(d, t) for t in standard_time_range]
            day_df_full = day_df.reindex(full_index)
            day_df_full['Volume'] = day_df_full['Volume'].fillna(0)
            day_df_full['cum_vol'] = day_df_full['Volume'].cumsum()
            day_df_full['time_str'] = day_df_full.index.strftime('%H:%M')
            daily_filled_data.append(day_df_full)
        
        if not daily_filled_data:
            return
            
        history_full = pd.concat(daily_filled_data)
        baseline_map = history_full.groupby('time_str')['cum_vol'].mean().to_dict()

        # ==============================================================================
        # BƯỚC 2: QUÉT INTRADAY
        # ==============================================================================
        
        df_intraday['time'] = pd.to_datetime(df_intraday['time'])
        if df_intraday['time'].dt.tz is not None:
            df_intraday['time'] = df_intraday['time'].dt.tz_localize(None)
            
        # Gộp Intraday về từng phút
        df_today_min = df_intraday.set_index('time').resample('1min').agg({
            'price': 'last',  
            'volume': 'sum'   
        }).dropna()
        
        df_today_min['cum_vol_today'] = df_today_min['volume'].cumsum()
        
        # --- VÒNG LẶP CHECK ---
        # Chỉ check những phút mới nhất hoặc check toàn bộ (ở đây code check toàn bộ intraday đã có)
        # Để tránh spam tin nhắn cũ, ta sẽ check từ dưới lên hoặc check hết nhưng lọc bằng alert_tracker
        
        for t, row in df_today_min.iterrows():
            # Check lại quota lần nữa (vì trong vòng lặp này có thể nó báo đủ 2 lần rồi)
            if len(alert_tracker[ticker][session]) >= 2:
                break 

            time_key = t.strftime('%H:%M')
            # [QUAN TRỌNG] CHECK TRÙNG THỜI GIAN:
            # Nếu giờ này đã nằm trong danh sách đã báo -> Bỏ qua
            if time_key in alert_tracker[ticker][session]:
                continue
            
            # Lấy dữ liệu
            price_now = row['price']
            vol_now = row['cum_vol_today']
            vol_ma20 = baseline_map.get(time_key, 0)
            
            # Logic so sánh
            is_vol_spike = False
            ratio = 0
            
            if vol_ma20 > 0:
                ratio = vol_now / vol_ma20
                if ratio >= vol_multiplier:
                    is_vol_spike = True
            
            is_price_break = price_now > price_threshold
            
            # 4. LOGIC CẢNH BÁO & GỬI TELEGRAM
            if is_vol_spike or is_price_break:

                # --- TẠO HASHTAG ---
                now_vn = datetime.now(VN_TZ)
                date_str = now_vn.strftime('%d_%m_%Y') 
                hashtag = f"#{date_str}_{ticker}"
                
                # Tạo nội dung tin nhắn
                msg = ""
                send_signal = False # Cờ để quyết định có gửi hay không

                # SỬA LẠI ĐOẠN NÀY ĐỂ HỢP VỚI DISCORD (Thay <b> bằng **)
                if is_vol_spike and is_price_break:
                    msg = (f"🔥🔥🔥 **{ticker}** | {time_key}\n"
                           f"**SUPER ALERT: GIÁ VÀ VOL ĐỀU NỔ!**\n"
                           f"💰 Giá: {price_now} (> {price_threshold})\n"
                           f"🚀 Vol tích lũy: {vol_now:,.0f} (x{ratio:.1f} MA20)\n"
                           f"{hashtag}")
                    send_signal = True
                
                elif is_vol_spike:
                    msg = (f"🚀 **{ticker}** | {time_key}\n"
                           f"**CẢNH BÁO VOL: Nổ Volume (x{ratio:.1f})**\n"
                           f"📊 Vol: {vol_now:,.0f} vs MA20: {vol_ma20:,.0f}\n"
                           f"💵 Giá hiện tại: {price_now}\n"
                           f"{hashtag}")
                    send_signal = True
                    
                elif is_price_break:
                    msg = (f"🔔 **{ticker}** | {time_key}\n"
                           f"**CẢNH BÁO GIÁ: Vượt ngưỡng {price_threshold}**\n"
                           f"💵 Giá hiện tại: {price_now}\n"
                           f"📊 Vol ratio: {ratio:.1f}x\n"
                           f"{hashtag}")
                    send_signal = True

                # Gửi Discord thay vì Telegram
                if send_signal:
                    print(f"Detect {ticker} at {time_key}. Sending Discord...") 
                    send_discord(msg) # Đổi tên hàm ở đây
                    
                    alert_tracker[ticker][session].append(time_key)
                    
    except Exception as e:
        print(f"Error processing {ticker}: {e}")

def get_stock_price(tickers, type='long', start=None, end=None, interval='1D', time_sleep=60):
    if start is None:
        start = '2000-01-01'
    if end is None:
        end = str(datetime.date.today())
    
    if isinstance(tickers, str):
        if tickers == 'VN30':
            cp = Listing().symbols_by_group('VN30').tolist() + ['VNINDEX']
        if tickers == 'VN100':
            cp = Listing().symbols_by_group('VN100').tolist() + ['VNINDEX']  
        if tickers == 'full':
            vn100 = Listing().symbols_by_group('VN100').tolist()
            cp = list(set(vn100) | set(full))
    elif isinstance(tickers, list):
        cp = tickers + ['VNINDEX']

    df = pd.DataFrame()
    parts = []
    for idx, symbol in enumerate(cp, start=1):
        def get_history_task():
            # Lưu ý: stock.quote.history đôi khi trả lỗi chứ không raise Exception,
            # nhưng vnstock bản mới thường raise Exception.
            return stock.quote.history(symbol=symbol, start=start, end=end, interval=interval)
        
        a = call_vnstock_safe(get_history_task)
        if a is None or a.empty:
            continue
        a.columns = [col.capitalize() for col in a.columns]
        a['ticker'] = symbol
        parts.append(a)
        if idx % 50 == 0:
            print(f"Đã lấy {idx} mã, tạm nghỉ {time_sleep} giây để tránh giới hạn request...")
            time.sleep(time_sleep)
    df = pd.concat(parts, ignore_index=True).set_index('Time')
    if type == 'width':
        df = df.pivot_table(index=df.index, columns='ticker')
        df.columns = df.columns.swaplevel(0, 1)
        df.columns.names = ['ticker', 'attribute']

    return df

def download_data():
    print("📥 Đang tải dữ liệu lịch sử (History Day/Minute)...")
    start = str(dt.date.today() - relativedelta(months=2))
    end = str(dt.date.today() - dt.timedelta(days=1))
    df_minute = get_stock_price(tickers=cp, start=start, end=end, interval='1m', time_sleep=60)
    time.sleep(60)
    df_day = get_stock_price(tickers=cp, start=start, end=end, interval='1D', time_sleep=60)

    print("✅ Đã tải xong dữ liệu!")
    # Return dummy data (Thay bằng df thật)
    return df_minute, df_day

def main():
    # 1. TẢI DATA
    df_minute_hist, df_day_hist = download_data()
    
    print("⏳ Bot đang chạy...")
    
    # Biến lưu trạng thái cũ để so sánh
    last_status = None 
    
    while True:
        now = datetime.now(VN_TZ)
        current_hm = now.hour * 100 + now.minute
        
        # --- LOGIC XỬ LÝ ---
        
        # CASE 1: CHỜ SÁNG (< 09:15)
        if current_hm < 915:
            # Chỉ in 1 lần duy nhất khi mới vào trạng thái này
            if last_status != "WAITING_MORNING":
                print(f"\n[{now.strftime('%H:%M')}] ☕ Chưa đến giờ mở cửa. Bot đang chờ đến 09:15...")
                last_status = "WAITING_MORNING"
            
            time.sleep(60) # Ngủ im lặng, không print gì cả
            
        # CASE 2: CHIẾN ĐẤU SÁNG (09:15 -> 11:30)
        elif 915 <= current_hm < 1130:
            if last_status != "SCANNING_AM":
                print(f"\n[{now.strftime('%H:%M')}] ☀️ BẮT ĐẦU PHIÊN SÁNG! Đang quét lệnh...")
                last_status = "SCANNING_AM"
            
            # Đoạn quét này giữ nguyên, nhưng dùng \r để nó chạy trên 1 dòng cho gọn
            print(f"[{now.strftime('%H:%M:%S')}] 🔄 Đang quét...", end='\r')
            
            for idx, symbol in enumerate(cp, start=1):
                notification(symbol, df_day_hist, df_minute_hist, price_threshold=1000)
                if idx % 50 == 0: time.sleep(time_sleep)
            
        # CASE 3: NGHỈ TRƯA (11:30 -> 13:00) - ĐÂY LÀ CHỖ BẠN CẦN
        elif 1130 <= current_hm < 1300:
            # Chỉ in ĐÚNG 1 LẦN khi bắt đầu nghỉ trưa
            if last_status != "LUNCH_BREAK":
                print(f"\n[{now.strftime('%H:%M')}] 🍱 Hết phiên sáng. Bot nghỉ trưa (Giữ data, chế độ im lặng).")
                last_status = "LUNCH_BREAK"
            
            # Bot ngủ im lặng, không spam log nữa
            time.sleep(60) 
            
        # CASE 4: CHIẾN ĐẤU CHIỀU (13:00 -> 15:00)
        elif 1300 <= current_hm < 1500:
            if last_status != "SCANNING_PM":
                print(f"\n[{now.strftime('%H:%M')}] 🌤️ BẮT ĐẦU PHIÊN CHIỀU! Tiếp tục quét...")
                last_status = "SCANNING_PM"
                
            print(f"[{now.strftime('%H:%M:%S')}] 🔄 Đang quét...", end='\r')
            for idx, symbol in enumerate(cp, start=1):
                notification(symbol, df_day_hist, df_minute_hist, price_threshold=1000)
                if idx % 50 == 0: time.sleep(time_sleep)

        # CASE 5: HẾT GIỜ
        elif current_hm >= 1500:
            print(f"\n[{now.strftime('%H:%M')}] 🏁 Hết phiên giao dịch. Bot tắt.")
            break

if __name__ == "__main__":
    # Logic download và chạy main
    # Lưu ý: Code main() ở câu trả lời trước tôi đã viết sẵn rồi
    # Bạn chỉ cần copy paste vào là chạy.
    # Nhớ thêm dòng này để test biến môi trường    
    # Giả lập main loop của câu trước
    main()