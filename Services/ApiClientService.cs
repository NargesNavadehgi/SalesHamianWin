using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json;
using SalesHamianWin;
using SalesHamianWin.Models;

namespace SalesHamianWin.Services
{
    public class ApiClientService
    {
        private readonly HttpClient _httpClient;
        private const string BaseUrl = "http://xx/api/ThirdPartySaleInfoDetailAPI/GetSaleInfoDetails";

        public ApiClientService(HttpClient httpClient)
        {
            _httpClient = httpClient;
        }

        public async Task<RootObject> GetSaleInfoDetails(string fromDate, string toDate)
        {
            try
            {
                // اصلاح URL - حذف اسلاش اضافه
                const string BaseUrl = "http://xx/api/ThirdPartySaleInfoDetailAPI/GetSaleInfoDetails";

                var parameters = new Dictionary<string, string>
        {
            { "fromDate", fromDate },
            { "toDate", toDate },
            { "userId", "xx" },  
            { "appUserName", "xx" }   
        };

                var queryString = string.Join("&", parameters.Select(p => $"{p.Key}={Uri.EscapeDataString(p.Value)}"));
                var fullUrl = $"{BaseUrl}?{queryString}";

                Console.WriteLine($"🔗 URL ارسالی: {fullUrl}");

                var response = await _httpClient.GetAsync(fullUrl);

                if (!response.IsSuccessStatusCode)
                {
                    var errorContent = await response.Content.ReadAsStringAsync();
                    Console.WriteLine($"❌ خطای سرور: {errorContent}");
                    throw new HttpRequestException($"HTTP Error: {(int)response.StatusCode} - {errorContent}");
                }

                var json = await response.Content.ReadAsStringAsync();
                Console.WriteLine($"✅ پاسخ موفق: {json.Substring(0, Math.Min(100, json.Length))}...");

                return JsonConvert.DeserializeObject<RootObject>(json);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"💥 خطا در دریافت داده: {ex}");
                throw;
            }
        }
        // دریافت اطلاعات حواله مرجوعی
        public async Task<List<SaleVoucherReturnInfo>> GetSaleVoucherReturnInfo(string fromDate, string toDate)
        {
            try
            {
                var parameters = new Dictionary<string, string>
        {
            { "fromDate", fromDate },
            { "toDate", toDate },
            { "appuserId", "570000013" },
            { "appUserName", "HPNI" }
        };

                var queryString = string.Join("&", parameters.Select(p => $"{p.Key}={Uri.EscapeDataString(p.Value)}"));
                var fullUrl = $"http://87.107.28.117:9096/api/ThirdPartySaleDetailAPI/GetSaleVoucherReturnInfoDetail?{queryString}";

                var response = await _httpClient.GetAsync(fullUrl);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                return JsonConvert.DeserializeObject<List<SaleVoucherReturnInfo>>(json);
            }
            catch (Exception ex)
            {
                // مدیریت خطاها
                throw;
            }
        }
        // دریافت اطلاعات مشتریان
        public async Task<List<CustomerInfo>> GetCustomersInfo(string fromDate, string toDate)
        {
            var parameters = new Dictionary<string, string>
    {
        { "fromDate", fromDate },
        { "toDate", toDate },
        { "appuserId", "xx" },
        { "appUserName", "xx" }
    };

            var queryString = string.Join("&", parameters.Select(p => $"{p.Key}={Uri.EscapeDataString(p.Value)}"));
            var fullUrl = $"http://xx/api/ThirdPartyCustomerAPI/GetCustomersInfoDetail?{queryString}";

            var response = await _httpClient.GetAsync(fullUrl);
            response.EnsureSuccessStatusCode();

            return JsonConvert.DeserializeObject<List<CustomerInfo>>(await response.Content.ReadAsStringAsync());
        }
    }
}
