using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Drawing;
using System.IO;
using System.Threading.Tasks;
using System.Windows.Forms;
using SalesHamianWin.Services;
using SalesHamianWin.Models;
using System.Net.Http;
using System.ComponentModel;

namespace SalesHamianWin
{
    public partial class Form1 : Form
    {
        private readonly ApiClientService _apiService;
        private readonly DataService _dataService;
        private readonly string _sourceConnectionString;
        private readonly string _targetConnectionString;
        private readonly HttpClient _httpClient;
        private BackgroundWorker _migrationWorker;

        // 🆕 اضافه کردن این دو فیلد به کلاس Form1
        private Button _btnTransferData;
        private Label _lblMigrationStatus;

        public Form1()
        {
            InitializeComponent();
            InitializeModernUI();

            _sourceConnectionString = @"Server=xx;Database=xx;User ID=xx;Password=xx";
            _targetConnectionString = @"Server=xx;Database=xx;User ID=xx;Password=xx";
            _httpClient = new HttpClient();
            _apiService = new ApiClientService(_httpClient);
            _dataService = new DataService(_sourceConnectionString, _targetConnectionString);

            InitializeMigrationWorker();
        }

        private void InitializeMigrationWorker()
        {
            _migrationWorker = new BackgroundWorker();
            _migrationWorker.WorkerReportsProgress = true;
            _migrationWorker.DoWork += MigrationWorker_DoWork;
            _migrationWorker.ProgressChanged += MigrationWorker_ProgressChanged;
            _migrationWorker.RunWorkerCompleted += MigrationWorker_RunWorkerCompleted;
        }

        private void InitializeModernUI()
        {
            // تنظیمات مدرن فرم
            this.Text = "🚀 سیستم مدیریت فروش حامیان";
            this.BackColor = Color.White;
            this.Size = new Size(1000, 700);
            this.StartPosition = FormStartPosition.CenterScreen;

            // تنظیمات کنترل‌های موجود
            ConfigureExistingControls();

            // اضافه کردن کنترل‌های جدید
            AddNewControls();
        }

        private void ConfigureExistingControls()
        {
            // برچسب برای تاریخ‌ها
            var lblFromDate = new Label()
            {
                Text = "از تاریخ:",
                Location = new Point(50, 107),
                Size = new Size(70, 25),
                Font = new Font("B Nazanin", 11, FontStyle.Bold),
                TextAlign = ContentAlignment.MiddleRight
            };

            var lblToDate = new Label()
            {
                Text = "تا تاریخ:",
                Location = new Point(200, 107),
                Size = new Size(70, 25),
                Font = new Font("B Nazanin", 11, FontStyle.Bold),
                TextAlign = ContentAlignment.MiddleRight
            };

            // تنظیمات textBox1 (از تاریخ)
            textBox1.Location = new Point(125, 105);
            textBox1.Size = new Size(70, 25);
            textBox1.Font = new Font("B Nazanin", 10);
            textBox1.Text = "1404/07/01";

            // تنظیمات textBox2 (تا تاریخ)
            textBox2.Location = new Point(275, 105);
            textBox2.Size = new Size(70, 25);
            textBox2.Font = new Font("B Nazanin", 10);
            textBox2.Text = "1404/07/30";

            // تنظیمات txtLog (لاگ)
            txtLog.Location = new Point(50, 330);
            txtLog.Size = new Size(900, 300);
            txtLog.Multiline = true;
            txtLog.ScrollBars = ScrollBars.Vertical;
            txtLog.Font = new Font("B Nazanin", 10);
            txtLog.BackColor = Color.FromArgb(248, 249, 250);

            // تنظیمات دکمه‌ها - با اندازه بزرگتر
            button1.Text = "📊 دریافت فروش";
            button1.Location = new Point(50, 150);
            button1.Size = new Size(150, 35);
            button1.BackColor = Color.FromArgb(40, 167, 69);
            button1.ForeColor = Color.White;
            button1.Font = new Font("B Nazanin", 10, FontStyle.Bold);
            button1.FlatStyle = FlatStyle.Flat;

            button2.Text = "🔄 دریافت مرجوعی";
            button2.Location = new Point(210, 150);
            button2.Size = new Size(150, 35);
            button2.BackColor = Color.FromArgb(255, 193, 7);
            button2.ForeColor = Color.White;
            button2.Font = new Font("B Nazanin", 10, FontStyle.Bold);
            button2.FlatStyle = FlatStyle.Flat;
            button2.Click += btnImportReturns_Click;

            button3.Text = "👥 دریافت مشتریان";
            button3.Location = new Point(370, 150);
            button3.Size = new Size(150, 35);
            button3.BackColor = Color.FromArgb(0, 123, 255);
            button3.ForeColor = Color.White;
            button3.Font = new Font("B Nazanin", 10, FontStyle.Bold);
            button3.FlatStyle = FlatStyle.Flat;
            button3.Click += btnImportCustomers_Click;

            // تنظیمات progressBar1
            progressBar1.Location = new Point(690, 235);
            progressBar1.Size = new Size(150, 10);
            progressBar1.Visible = false;
            progressBar1.Style = ProgressBarStyle.Continuous;

            // اضافه کردن کنترل‌های جدید به فرم
            this.Controls.Add(lblFromDate);
            this.Controls.Add(lblToDate);
        }

        private void AddNewControls()
        {
            // عنوان برنامه
            var lblTitle = new Label()
            {
                Text = "🚀 سیستم مدیریت فروش حامیان",
                Font = new Font("B Nazanin", 16, FontStyle.Bold),
                ForeColor = Color.FromArgb(0, 122, 204),
                Size = new Size(400, 40),
                Location = new Point(300, 30),
                TextAlign = ContentAlignment.MiddleCenter
            };

            // دکمه‌های پاک کردن
            var btnClearSales = CreateModernButton("🗑️ پاک کردن فروش", new Point(50, 195),
                Color.FromArgb(220, 53, 69));
            btnClearSales.Size = new Size(150, 35);

            var btnClearReturns = CreateModernButton("🗑️ پاک کردن مرجوعی", new Point(210, 195),
                Color.FromArgb(220, 53, 69));
            btnClearReturns.Size = new Size(150, 35);

            var btnClearCustomers = CreateModernButton("🗑️ پاک کردن مشتریان", new Point(370, 195),
                Color.FromArgb(220, 53, 69));
            btnClearCustomers.Size = new Size(150, 35);

            // دکمه آمار
            var btnStatistics = CreateModernButton("📊 نمایش آمار", new Point(530, 150),
                Color.FromArgb(108, 117, 125));
            btnStatistics.Size = new Size(150, 80);

            // 🆕 دکمه انتقال داده به دیتابیس
            _btnTransferData = CreateModernButton("🚀 انتقال دیتا به دیتابیس", new Point(690, 150),
                Color.FromArgb(111, 66, 193));
            _btnTransferData.Size = new Size(150, 80);
            _btnTransferData.Click += btnTransferData_Click;

            // 🆕 برچسب وضعیت انتقال
            _lblMigrationStatus = new Label()
            {
                Text = "آماده",
                Location = new Point(690, 250),
                Size = new Size(150, 20),
                Font = new Font("B Nazanin", 9),
                ForeColor = Color.FromArgb(108, 117, 125),
                TextAlign = ContentAlignment.MiddleCenter
            };

            // برچسب لاگ
            var lblLog = new Label()
            {
                Text = "لاگ عملیات:",
                Location = new Point(50, 310),
                Size = new Size(100, 20),
                Font = new Font("B Nazanin", 11, FontStyle.Bold),
                ForeColor = Color.FromArgb(108, 117, 125)
            };

            // اضافه کردن رویدادها
            btnClearSales.Click += btnClearSales_Click;
            btnClearReturns.Click += btnClearReturns_Click;
            btnClearCustomers.Click += btnClearCustomers_Click;
            btnStatistics.Click += btnStatistics_Click;

            // اضافه کردن به فرم
            this.Controls.AddRange(new Control[]
            {
                lblTitle,
                btnClearSales,
                btnClearReturns,
                btnClearCustomers,
                btnStatistics,
                _btnTransferData,
                _lblMigrationStatus,
                lblLog
            });
        }

        private Button CreateModernButton(string text, Point location, Color backColor)
        {
            return new Button
            {
                Text = text,
                Size = new Size(150, 35),
                Location = location,
                BackColor = backColor,
                ForeColor = Color.White,
                Font = new Font("B Nazanin", 10, FontStyle.Bold),
                FlatStyle = FlatStyle.Flat,
                Cursor = Cursors.Hand
            };
        }

        // 🆕 متدهای مربوط به انتقال داده
        private void btnTransferData_Click(object sender, EventArgs e)
        {
            try
            {
                var result = MessageBox.Show(
                    "آیا از انتقال داده‌ها به دیتابیس اصلی اطمینان دارید؟\nاین عملیات ممکن است زمان‌بر باشد.",
                    "تأیید انتقال داده",
                    MessageBoxButtons.YesNo,
                    MessageBoxIcon.Question);

                if (result == DialogResult.Yes)
                {
                    // غیرفعال کردن دکمه در حین انتقال
                    _btnTransferData.Enabled = false;

                    // نمایش Progress Bar
                    progressBar1.Visible = true;
                    progressBar1.Value = 0;

                    _lblMigrationStatus.Text = "در حال انتقال...";
                    _lblMigrationStatus.ForeColor = Color.Blue;

                    AppendLog("🚀 شروع انتقال داده به دیتابیس اصلی...");

                    // 🆕 ارسال mode به عنوان آرگومان - این خطا را برطرف می‌کند
                    _migrationWorker.RunWorkerAsync("finalQueriesOnly");
                }
            }
            catch (Exception ex)
            {
                AppendLog($"❌ خطا در شروع انتقال: {ex.Message}");
                _btnTransferData.Enabled = true;
            }
        }

        private void MigrationWorker_DoWork(object sender, DoWorkEventArgs e)
        {
            try
            {
                string mode = e.Argument as string;

                _migrationWorker.ReportProgress(10, "در حال بررسی داده‌ها...");

                if (mode == "finalQueriesOnly")
                {
                    _migrationWorker.ReportProgress(20, "در حال اجرای کوئری اول (مشتریان)...");

                    // اجرای کوئری اول
                    _dataService.ExecuteQuery1().Wait(); // استفاده از Wait() چون در BackgroundWorker هستیم

                    _migrationWorker.ReportProgress(60, "در حال اجرای کوئری دوم (فروش)...");

                    // اجرای کوئری دوم
                    _dataService.ExecuteQuery2().Wait();

                    _migrationWorker.ReportProgress(100, "کوئری‌های نهایی با موفقیت اجرا شد");
                }
                else
                {
                    _migrationWorker.ReportProgress(50, "اجرای کامل انتقال داده در حال توسعه...");
                    _migrationWorker.ReportProgress(100, "اجرا تکمیل شد");
                }
            }
            catch (Exception ex)
            {
                // لاگ خطاهای دقیق‌تر
                string errorDetails = $"خطا در عملیات انتقال: {ex.Message}";
                if (ex.InnerException != null)
                {
                    errorDetails += $"\nخطای داخلی: {ex.InnerException.Message}";
                }
                throw new Exception(errorDetails);
            }
        }

        private void MigrationWorker_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            progressBar1.Value = e.ProgressPercentage;

            if (e.UserState != null)
            {
                _lblMigrationStatus.Text = e.UserState.ToString();
                _lblMigrationStatus.ForeColor = e.ProgressPercentage < 100 ? Color.Blue : Color.Green;
                AppendLog(e.UserState.ToString());
            }
        }

        private void MigrationWorker_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            // فعال کردن مجدد دکمه
            _btnTransferData.Enabled = true;

            // مخفی کردن Progress Bar
            progressBar1.Visible = false;

            if (e.Error != null)
            {
                AppendLog($"❌ خطا در انتقال داده: {e.Error.Message}");
                _lblMigrationStatus.Text = "خطا در انتقال";
                _lblMigrationStatus.ForeColor = Color.Red;
            }
            else
            {
                AppendLog("✅ انتقال داده با موفقیت completed شد");
                _lblMigrationStatus.Text = "انتقال کامل شد";
                _lblMigrationStatus.ForeColor = Color.Green;

                MessageBox.Show("انتقال داده با موفقیت انجام شد", "موفقیت",
                    MessageBoxButtons.OK, MessageBoxIcon.Information);
            }
        }



        // 📋 متدهای اصلی موجود
        private async void button1_Click(object sender, EventArgs e)
        {
            await ImportData("Sales");
        }

        private async void btnImportReturns_Click(object sender, EventArgs e)
        {
            await ImportData("Returns");
        }

        private async void btnImportCustomers_Click(object sender, EventArgs e)
        {
            await ImportData("Customers");
        }

        // 🔥 متدهای جدید برای پاک کردن داده‌ها
        private async void btnClearSales_Click(object sender, EventArgs e)
        {
            await ClearData("Sales");
        }

        private async void btnClearReturns_Click(object sender, EventArgs e)
        {
            await ClearData("Returns");
        }

        private async void btnClearCustomers_Click(object sender, EventArgs e)
        {
            await ClearData("Customers");
        }

        // 📊 متد برای نمایش آمار
        private async void btnStatistics_Click(object sender, EventArgs e)
        {
            await UpdateStatistics();
        }

        // 🎯 متدهای کمکی
        private async Task ImportData(string dataType)
        {
            try
            {
                AppendLog($"🔄 در حال دریافت {GetPersianName(dataType)}...");

                int recordCount = 0;
                switch (dataType)
                {
                    case "Sales":
                        var salesData = await _apiService.GetSaleInfoDetails(textBox1.Text, textBox2.Text);
                        recordCount = await _dataService.SaveSalesData(salesData.ThirdPartySaleInfoDetailModels);
                        break;

                    case "Returns":
                        var returnsData = await _apiService.GetSaleVoucherReturnInfo(textBox1.Text, textBox2.Text);
                        recordCount = await _dataService.SaveReturnInfo(returnsData);
                        break;

                    case "Customers":
                        var customersData = await _apiService.GetCustomersInfo(textBox1.Text, textBox2.Text);
                        recordCount = await _dataService.SaveCustomers(customersData);
                        break;
                }

                AppendLog($"✅ {recordCount} رکورد {GetPersianName(dataType)} ذخیره شد");
            }
            catch (Exception ex)
            {
                AppendLog($"❌ خطا در دریافت {GetPersianName(dataType)}: {ex.Message}");
            }
        }

        private async Task ClearData(string dataType)
        {
            try
            {
                var result = MessageBox.Show(
                    $"آیا از پاک کردن داده‌های {GetPersianName(dataType)} مطمئن هستید؟",
                    "تأیید پاک کردن",
                    MessageBoxButtons.YesNo,
                    MessageBoxIcon.Warning);

                if (result == DialogResult.Yes)
                {
                    AppendLog($"🗑️ در حال پاک کردن {GetPersianName(dataType)}...");

                    int deletedCount = 0;
                    switch (dataType)
                    {
                        case "Sales":
                            deletedCount = await _dataService.ClearSalesData(textBox1.Text, textBox2.Text);
                            break;

                        case "Returns":
                            deletedCount = await _dataService.ClearReturnData(textBox1.Text, textBox2.Text);
                            break;

                        case "Customers":
                            deletedCount = await _dataService.ClearCustomersData();
                            break;
                    }

                    AppendLog($"✅ {deletedCount} رکورد از {GetPersianName(dataType)} پاک شد");
                }
            }
            catch (Exception ex)
            {
                AppendLog($"❌ خطا در پاک کردن {GetPersianName(dataType)}: {ex.Message}");
            }
        }

        private async Task UpdateStatistics()
        {
            try
            {
                AppendLog("📊 در حال دریافت آمار...");

                var salesCount = await _dataService.GetSalesCount();
                var returnsCount = await _dataService.GetReturnsCount();
                var customersCount = await _dataService.GetCustomersCount();

                AppendLog("📈 آمار فعلی سیستم:");
                AppendLog($"   • فروش: {salesCount} رکورد");
                AppendLog($"   • مرجوعی: {returnsCount} رکورد");
                AppendLog($"   • مشتریان: {customersCount} رکورد");
            }
            catch (Exception ex)
            {
                AppendLog($"❌ خطا در دریافت آمار: {ex.Message}");
            }
        }

        private void AppendLog(string message)
        {
            if (txtLog.InvokeRequired)
            {
                txtLog.Invoke(new Action<string>(AppendLog), message);
            }
            else
            {
                txtLog.AppendText($"{DateTime.Now:HH:mm:ss} - {message}{Environment.NewLine}");
                txtLog.ScrollToCaret();
            }
        }

        private string GetPersianName(string dataType)
        {
            if (dataType == "Sales")
                return "فروش";
            else if (dataType == "Returns")
                return "مرجوعی";
            else if (dataType == "Customers")
                return "مشتریان";
            else
                return dataType;
        }
    }
}
