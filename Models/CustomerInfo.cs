using Newtonsoft.Json;

namespace SalesHamianWin.Models
{
    public class CustomerInfo
    {
        [JsonProperty("ID")]
        public long Id { get; set; }

        [JsonProperty("CustCode")]
        public string CustCode { get; set; }

        [JsonProperty("CustomerName")]
        public string CustomerName { get; set; }

        [JsonProperty("CustActName")]
        public string CustActName { get; set; }

        [JsonProperty("CustLevelName")]
        public string CustLevelName { get; set; }

        [JsonProperty("CustCtgrName")]
        public string CustCtgrName { get; set; }

        [JsonProperty("AreaName")]
        public string AreaName { get; set; }

        [JsonProperty("StateName")]
        public string StateName { get; set; }
    }
}