using System;
using Newtonsoft.Json;

namespace SalesHamianWin.Models
{
    public class SaleVoucherReturnInfo
    {
        [JsonProperty("CustCode")]
        public long CustCode { get; set; }

        [JsonProperty("SaleVocherNo")]
        public int SaleVocherNo { get; set; }

        [JsonProperty("DealerCode")]
        public string DealerCode { get; set; }

        [JsonProperty("DealerName")]
        public string DealerName { get; set; }

        [JsonProperty("SupervisorCode")]
        public string SupervisorCode { get; set; }

        [JsonProperty("SupervisorName")]
        public string SupervisorName { get; set; }

        [JsonProperty("GoodsCode")]
        public string GoodsCode { get; set; }

        [JsonProperty("GoodsName")]
        public string GoodsName { get; set; }

        [JsonProperty("TotalAmountRetVocher")]
        public decimal TotalAmountRetVocher { get; set; }

        [JsonProperty("DiscountRetVocher")]
        public double DiscountRetVocher { get; set; }

        [JsonProperty("Dis1RetVocher")]
        public decimal Dis1RetVocher { get; set; }

        [JsonProperty("Dis2RetVocher")]
        public decimal Dis2RetVocher { get; set; }

        [JsonProperty("Dis3RetVocher")]
        public decimal Dis3RetVocher { get; set; }

        [JsonProperty("AddAmountRetVocher")]
        public decimal AddAmountRetVocher { get; set; }

        [JsonProperty("Add1RetVocher")]
        public decimal Add1RetVocher { get; set; }

        [JsonProperty("Add2RetVocher")]
        public decimal Add2RetVocher { get; set; }

        [JsonProperty("TotalAmountNutRetVocher")]
        public decimal TotalAmountNutRetVocher { get; set; }

        [JsonProperty("RetVocherQty")]
        public decimal RetVocherQty { get; set; }

        [JsonProperty("RetPrizeVocherQty")]
        public decimal RetPrizeVocherQty { get; set; }

        [JsonProperty("VocherQty")]
        public decimal VocherQty { get; set; }

        [JsonProperty("PrizevocherQty")]
        public decimal PrizevocherQty { get; set; }

        [JsonProperty("PaymentUsanceName")]
        public string PaymentUsanceName { get; set; }

        [JsonProperty("RetCauseName")]
        public string RetCauseName { get; set; }
    }
}