using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace SalesHamianWin.Models
{
    public class RootObject
    {
        public int Id { get; set; }
        public List<SaleInfoDetail> ThirdPartySaleInfoDetailModels { get; set; }
        public object ThirdPartyResult { get; set; }
    }
    public class SaleInfoDetail
    {
        public int Id { get; set; }
        public int? SaleNo { get; set; }
        public string SaleDate { get; set; }
        public int? SaleVocherNo { get; set; }
        public string SaleVocherDate { get; set; }
        public string CustCode { get; set; }
        public string CustomerName { get; set; }
        public string StoreName { get; set; }
        public string KOB { get; set; }
        public string DealerName { get; set; }
        public string DealerCode { get; set; }
        public string DCName { get; set; }
        public string BrandName { get; set; }
        public string Category { get; set; }
        public string GoodsCode { get; set; }
        public string GoodsName { get; set; }
        public string Barcode { get; set; }
        public string Barcode2 { get; set; }
        public string SaleIndex { get; set; }
        public float CartonType { get; set; }
        public float SaleAndPrizeCartonQty { get; set; }
        public float SaleAndPrizeQty { get; set; }
        public float PrizeCartonQty { get; set; }
        public float PrizeQty { get; set; }
        public float GoodsWeight { get; set; }
        public float CustPrice { get; set; }
        public float Amount { get; set; }
        public float Discount { get; set; }
        public float AddAmount { get; set; }
        public float AmountMinusDiscount { get; set; }
        public float AmountNut { get; set; }
        public string PaymentTypeName { get; set; }
        public float Dis1 { get; set; }
        public float Dis2 { get; set; }
        public float Dis3 { get; set; }
        public float Add1 { get; set; }
        public float Add2 { get; set; }
        public string SupervisorName { get; set; }
        public string SupervisorCode { get; set; }
        public string AreaName { get; set; }
        public string StateName { get; set; }
        public int AccYear { get; set; }
        public int SolarMonthId { get; set; }
        public string SolarStrMonth { get; set; }
        public string SolarDay { get; set; }
        public string SolarYearMonth { get; set; }
        public string YearMonth { get; set; }
    }
}