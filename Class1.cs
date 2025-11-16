//using System;
//using System.Collections.Generic;
//using System.Net.Http;
//using System.Windows.Forms;
//using SalesTehranBouranWin.Models;
//using SalesTehranBouranWin.Services;

//namespace SalesHamianWin
//{
//    public partial class Form1 : Form
//    {
 
//        private readonly string _connectionString = @"Server=10.0.0.44\biinstance;Database=Hamian;User ID=sa;Password=123456";

//        public Form1()
//        {
//            InitializeComponent();
//            _apiService = new ApiClientService(new HttpClient());
//            _dataService = new DataService(_connectionString);
//        }

//        private async void button1_Click(object sender, EventArgs e)
//        {
//            try
//            {
//                var parameters = new Dictionary<string, string>
//                {
//                    { "fromDate", textBox1.Text },
//                    { "toDate", textBox2.Text },
//                    { "userId", "543" },
//                    { "appUserName", "parshayan" }
//                };

//                var apiData = await _apiService.GetSaleInfoDetails(parameters);
//                var recordCount = await _dataService.SaveSalesData(apiData.ThirdPartySaleInfoDetailModels);

//                textBox3.Text = $"Data imported successfully. Total records: {recordCount}";
//            }
//            catch (Exception ex)
//            {
//                textBox3.Text = $"Error: {ex.Message}";
//            }
//        }
//    }
//}