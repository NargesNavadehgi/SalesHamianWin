using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using SalesHamianWin.Models;
using Dapper;

namespace SalesHamianWin.Services
{
    public class DataService
    {
        private readonly string _sourceConnectionString; // برای Hamian
        private readonly string _targetConnectionString; // برای DSDB_HPNI

        public DataService(string sourceConnectionString, string targetConnectionString)
        {
            _sourceConnectionString = sourceConnectionString;
            _targetConnectionString = targetConnectionString;
        }
        // 🆕 متد برای اجرای اسکریپت روی دیتابیس مقصد
        public async Task ExecuteSqlScriptOnTarget(string script)
        {
            using (SqlConnection connection = new SqlConnection(_targetConnectionString))
            {
                await connection.OpenAsync();

                // اضافه کردن event handler برای دریافت پیام‌ها
                connection.InfoMessage += (sender, e) =>
                {
                    foreach (SqlError error in e.Errors)
                    {
                        Console.WriteLine($"SQL Message: {error.Message}");
                    }
                };

                string[] scriptBatches = script.Split(new[] { "GO" }, StringSplitOptions.RemoveEmptyEntries);

                foreach (string batch in scriptBatches)
                {
                    if (!string.IsNullOrWhiteSpace(batch.Trim()))
                    {
                        using (SqlCommand command = new SqlCommand(batch, connection))
                        {
                            command.CommandTimeout = 3600;
                            try
                            {
                                int rowsAffected = await command.ExecuteNonQueryAsync();
                                Console.WriteLine($"Rows affected: {rowsAffected}");
                            }
                            catch (SqlException ex)
                            {
                                Console.WriteLine($"SQL Error: {ex.Message}");
                                throw;
                            }
                        }
                    }
                }
            }
        }

        // 🆕 متد برای خواندن داده از دیتابیس مبدا
        public async Task<DataTable> ExecuteQueryOnSource(string query)
        {
            using (var connection = new SqlConnection(_sourceConnectionString))
            {
                await connection.OpenAsync();
                using (var command = new SqlCommand(query, connection))
                {
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        var dataTable = new DataTable();
                        dataTable.Load(reader);
                        return dataTable;
                    }
                }
            }
        }

        // 🆕 متدهای جدید برای انتقال داده
        public async Task<bool> ExecuteFinalQueriesOnly()
        {
            try
            {
                await ExecuteFinalQueries();
                return true;
            }
            catch (Exception ex)
            {
                throw new Exception($"خطا در اجرای کوئری‌های نهایی: {ex.Message}");
            }
        }

        // 🆕 متد برای اجرای دو کوئری نهایی
        private async Task ExecuteFinalQueries()
        {
            await ExecuteQuery1();
            await ExecuteQuery2();
        }

        // 🆕 کوئری اول
        public async Task ExecuteQuery1()
        {
            string query1 = @"
USE [DSDB_HPNI]
SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET NUMERIC_ROUNDABORT OFF;

disable trigger [GNR].[TRG_VN_LockCN_tblCust] on gnr.tblcust;
disable trigger [GNR].[trg_Replication_tblCust_INSERT] on gnr.tblcust;
disable trigger [trg_tblCust_Checkascii] on gnr.tblcust;

-- تعریف متغیرها
DECLARE @SourceId INT
DECLARE @NewId INT
DECLARE @NewCustCode NVARCHAR(2000)
DECLARE @CustomerName NVARCHAR(255)
declare @CustCode NVARCHAR(2000)
DECLARE @StateName NVARCHAR(255)
DECLARE @AreaName NVARCHAR(255)
DECLARE @CustCtgrName NVARCHAR(255)
DECLARE @CustActName NVARCHAR(255)
DECLARE @CustLevelName NVARCHAR(255)
declare @CustCtgrRef int 
declare @CustActRef int 
declare @AreaRef int 

-- شروع تراکنش برای امنیت
BEGIN TRANSACTION

BEGIN TRY
    -- تعریف Cursor برای خواندن رکوردها از جدول مبدا
    DECLARE customer_cursor CURSOR LOCAL FOR 
    SELECT 
        ID,
        CustomerName ,
		CustCode,
        StateName,
        AreaName,
        CustCtgrName,
		(SELECT TOP 1 ID FROM SLE.tblCustCtgrSle WHERE [CustCtgrName] = s.CustCtgrName) as CustCtgrRef,
		(SELECT TOP 1 ID FROM [GNR].[tblCustAct] WHERE [CustActName] = s.CustActName) as  [CustActRef],
		(SELECT TOP 1 ID FROM [GNR].[tblArea] WHERE LTRIM(RTRIM([AreaName])) = LTRIM(RTRIM(s.AreaName))) as  [AreaRef], 
        CustActName,
        CustLevelName
    FROM [Hamian].dbo.Customers S
    WHERE NOT EXISTS (
        SELECT 1 FROM [GNR].[tblCust] T 
        WHERE T.ID = S.ID OR T.Comment = S.CustCode ---- Map CustCode
    ) 

    OPEN customer_cursor
    FETCH NEXT FROM customer_cursor INTO @SourceId, @CustomerName,@CustCode, @StateName, @AreaName, @CustCtgrName,@CustCtgrRef,@CustActRef,@AreaRef, @CustActName, @CustLevelName

    -- حلقه برای پردازش هر رکورد
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- گرفتن NewId از SP برای هر رکورد
        EXEC gnr.uspGetNextId 'gnr.tblCust', @NewId OUTPUT

        -- گرفتن CustCode از SP برای هر رکورد
        EXEC GNR.UspGenCustCode 
            @DC = 3, -- DCRef  --- تهران بوران
            @CC = @CustCtgrRef,--CustCtgrRef
            @CA = @CustActRef, --CustActRef
            @AR = @AreaRef,--AreaRef
            @CRT = 2,--CustRegType
            @SP = NULL,
            @DP = NULL,
            @RP = NULL,
            @GeneratedCode = @NewCustCode OUTPUT

        -- درج رکورد با مقادیر تولید شده
        INSERT INTO [GNR].[tblCust] (
            [ID],
            [CustCode],
            [FirstName],
            [LastName],
            [RealName],
            [CustType],
            [CustRegType],
            [Status],
            [EconCode],
            [Phone],
            [FaxNo],
            [Mobile],
            [Email],
            [StateRef],
            [AreaRef],
            [DCRef],
            [PostCode],
            [EftDate],
            [CustCtgrRef],
            [CustActRef],
            [BedCredit],
            [AsnCredit],
            [MaxSaleM],
            [MaxFactM],
            [MaxSaleY],
            [Comment],
            [MaxFactY],
            [MaxRetSaleM],
            [MaxRetFactM],
            [MaxRetSaleY],
            [MaxRetFactY],
            [Address],
            [OldCode],
            [ChangeDate],
            [SalePathRef],
            [DistPathRef],
            [RcptPathRef],
            [CityZone],
            [Address2],
            [PayableTypes],
            [StoreArea],
            [CustLevelRef],
            [OwnerTypeRef],
            [InActiveCauseRef],
            [InActiveDate],
            [RentalStartDate],
            [RentalEndDate],
            [SalePriority],
            [DistPriority],
            [RcptPriority],
            [EssentialBedCredit],
            [GuranteeBedCredit],
            [VolumeBedCredit],
            [EssentialAsnCredit],
            [GuranteeAsnCredit],
            [VolumeAsnCredit],
            [EssentialBedCredit_Desc],
            [GuranteeBedCredit_Desc],
            [VolumeBedCredit_Desc],
            [EssentialAsnCredit_Desc],
            [GuranteeAsnCredit_Desc],
            [VolumeAsnCredit_Desc],
            [EssentialGrade],
            [EssentialClass],
            [VolumeGrade],
            [VolumeClass],
            [HasBedCredit],
            [HasAsnCredit],
            [Latitude],
            [Longitude],
            [ExpiryDate],
            [CityArea],
            [Username],
            [Password],
            [OpenInvoiceDays],
            [OpenInvoiceAmount],
            [OpenInvoiceQty],
            [UserRef],
            [HostName],
            [ModifiedDate],
            [ContactId],
            [DlCode],
            [NationalCode],
            [CustGroupRef],
            [CustGUID],
            [CustRegisterNo],
            [Phone2],
            [CreationDate],
            [CreationBy],
            [ModifiedBy],
            [WinUserName],
            [SQLUserName],
            [ApplicationName],
            [TelePriority],
            [TelePathRef],
            [ParentCustomerId],
            [IgnoreLocation],
            [IsParent],
            [ParentType],
            [ParentCustomerType],
            [SystemId],
            [RoleCode],
            [LocationParentCustomerId],
            [AccountingCustGroupRef],
            [ManualDLselection],
            [IsMergeRetSaleMoadian]
        )
        VALUES (
            @NewId,  -- ID تولید شده از SP
            @NewCustCode,  -- CustCode تولید شده از SP
            @CustomerName,  -- [FirstName]
            @CustomerName,  -- [LastName]  
            @CustomerName,  -- [RealName]
            1,  -- [CustType]
            2,  -- [CustRegType] --- نوع ثبتی حقیقی
            1,  -- [Status]
            NULL,  -- [EconCode]
            NULL,  -- [Phone]
            NULL,  -- [FaxNo]
            NULL,  -- [Mobile]
            NULL,  -- [Email]
            (SELECT TOP 1 ID FROM [GNR].[tblState] WHERE [StateName] = @StateName),  -- [StateRef]
            (SELECT TOP 1 ID FROM [GNR].[tblArea] WHERE [AreaName] = @AreaName),  -- [AreaRef]
            3,  -- [DCRef]
            NULL,  -- [PostCode]
            FORMAT(GETDATE(), 'yyyy/MM/dd', 'FA'),  -- [EftDate]
            (SELECT TOP 1 ID FROM SLE.tblCustCtgrSle WHERE [CustCtgrName] = @CustCtgrName),  -- [CustCtgrRef]
            (SELECT TOP 1 ID FROM [GNR].[tblCustAct] WHERE [CustActName] = @CustActName),  -- [CustActRef]
            0,  -- [BedCredit]
            0,  -- [AsnCredit]
            0,  -- [MaxSaleM]
            0,  -- [MaxFactM]
            0,  -- [MaxSaleY]
            @CustCode,  -- [Comment]
            0,  -- [MaxFactY]
            0,  -- [MaxRetSaleM]
            0,  -- [MaxRetFactM]
            0,  -- [MaxRetSaleY]
            0,  -- [MaxRetFactY]
            NULL,  -- [Address]
            NULL,  -- [OldCode]
            FORMAT(GETDATE(), 'yyyy/MM/dd', 'FA'),  -- [ChangeDate]
            NULL,  -- [SalePathRef]
            NULL,  -- [DistPathRef]
            NULL,  -- [RcptPathRef]
            NULL,  -- [CityZone]
            NULL,  -- [Address2]
            1047,  -- [PayableTypes]
            NULL,  -- [StoreArea]
            null ,--(SELECT TOP 1 ID FROM [GNR].[tblCustLevel] WHERE [CustLevelName] = @CustLevelName),  -- [CustLevelRef]
            NULL,  -- [OwnerTypeRef]
            NULL,  -- [InActiveCauseRef]
            NULL,  -- [InActiveDate]
            NULL,  -- [RentalStartDate]
            NULL,  -- [RentalEndDate]
            0,  -- [SalePriority]
            NULL,  -- [DistPriority]
            NULL,  -- [RcptPriority]
            0,  -- [EssentialBedCredit]
            0,  -- [GuranteeBedCredit]
            0,  -- [VolumeBedCredit]
            0,  -- [EssentialAsnCredit]
            0,  -- [GuranteeAsnCredit]
            0,  -- [VolumeAsnCredit]
            NULL,  -- [EssentialBedCredit_Desc]
            NULL,  -- [GuranteeBedCredit_Desc]
            NULL,  -- [VolumeBedCredit_Desc]
            NULL,  -- [EssentialAsnCredit_Desc]
            NULL,  -- [GuranteeAsnCredit_Desc]
            NULL,  -- [VolumeAsnCredit_Desc]
            NULL,  -- [EssentialGrade]
            NULL,  -- [EssentialClass]
            NULL,  -- [VolumeGrade]
            NULL,  -- [VolumeClass]
            1,  -- [HasBedCredit]
            1,  -- [HasAsnCredit]
            NULL,  -- [Latitude]
            NULL,  -- [Longitude]
            NULL,  -- [ExpiryDate]
            NULL,  -- [CityArea]
            NULL,  -- [Username]
            NULL,  -- [Password]
            0,  -- [OpenInvoiceDays]
            0,  -- [OpenInvoiceAmount]
            0,  -- [OpenInvoiceQty]
            2,  -- [UserRef] ----- مغادل VnAdmin
            'HPNI',  -- [HostName]
            GETDATE(),  -- [ModifiedDate]
            NULL,  -- [ContactId]
            NULL,  -- [DlCode]
            NULL,  -- [NationalCode]
            NULL,  -- [CustGroupRef]
            NEWID(),  -- [CustGUID]
            NULL,  -- [CustRegisterNo]
            NULL,  -- [Phone2]
            GETDATE(),  -- [CreationDate]
            2,  -- [CreationBy]
            2,  -- [ModifiedBy]
            NULL,  -- [WinUserName]
            NULL,  -- [SQLUserName]
            'Varanegar SDS.Net,5.9.0.300,2',  -- [ApplicationName]
            NULL,  -- [TelePriority]
            NULL,  -- [TelePathRef]
            NULL,  -- [ParentCustomerId]
            NULL,  -- [IgnoreLocation]
            0,  -- [IsParent]
            NULL,  -- [ParentType]
            NULL,  -- [ParentCustomerType]
            NULL,  -- [SystemId]
            NULL,  -- [RoleCode]
            NULL,  -- [LocationParentCustomerId]
            NULL,  -- [AccountingCustGroupRef]
            NULL,  -- [ManualDLselection]
            0  -- [IsMergeRetSaleMoadian]
        )

        -- خواندن رکورد بعدی
        FETCH NEXT FROM customer_cursor INTO @SourceId, @CustomerName,@CustCode, @StateName, @AreaName, @CustCtgrName,@CustCtgrRef,@CustActRef,@AreaRef, @CustActName, @CustLevelName
    END

    -- بستن Cursor
    CLOSE customer_cursor
    DEALLOCATE customer_cursor

    -- تایید تراکنش
    COMMIT TRANSACTION
    PRINT 'تمام رکوردها با موفقیت درج شدند'

END TRY
BEGIN CATCH
    -- در صورت خطا، بستن Cursor و Rollback
    IF CURSOR_STATUS('local', 'customer_cursor') >= 0
    BEGIN
        CLOSE customer_cursor
        DEALLOCATE customer_cursor
    END
    
    ROLLBACK TRANSACTION
    PRINT 'خطا در درج رکوردها: ' + ERROR_MESSAGE()
END CATCH";

            await ExecuteSqlScript(query1);
        }

        // 🆕 کوئری دوم
        public async Task ExecuteQuery2()
        {
            string query2 = @"
SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET NUMERIC_ROUNDABORT OFF;

USE [DSDB_HPNI];

-- غیرفعال کردن triggerها
DISABLE TRIGGER [TRG_VN_LockCN_tblOrderHdr] ON [SLE].[tblOrderHdr];
DISABLE TRIGGER [TRG_VN_LockCN_tblSaleHdr] ON [SLE].[tblSaleHdr];
DISABLE TRIGGER [trg_VN_Replication_tblorderhdr_UPDATE] ON [SLE].[tblOrderHdr];
DISABLE TRIGGER [Trg_tblSaleItm_UpdateStockGoods] ON [SLE].[tblSaleItm];

DECLARE @gtdt datetime, @Cnt_Hdr int, @SaleOfficeRef int, @Cnt_Itm int;
DECLARE @NewId_Hdr int, @NewId_Itm int, @AccYear int, @FreeInvoiceNo int;
DECLARE @DCRef int, @Today varchar(10), @SqlStr varchar(max), @IsConfirm int;
DECLARE @FreeInvoiceHdrRef int, @Torderno varchar(20), @Total int, @ErrStr nvarchar(max), @Comment int, @CustCode int,@DealerName nvarchar(max) 

SELECT @Today = gnr.DateTimeToSolar(GETDATE(),'yyyy/mm/dd');

-- حذف جداول موقت اگر وجود دارند
IF OBJECT_ID('tempdb.dbo.#tmp01', 'U') IS NOT NULL DROP TABLE #tmp01;
IF OBJECT_ID('tempdb.dbo.#tmp02', 'U') IS NOT NULL DROP TABLE #tmp02;
IF OBJECT_ID('tempdb.dbo.#tmp03', 'U') IS NOT NULL DROP TABLE #tmp03;

-- ایجاد جداول موقت برای داده‌های فروش
SELECT 
    TorderNo,
    FreeInvoiceDate,
    DCCode,
    StockDCCode,
    CustCode,
    BuyTypeCode,
    PaymentUsanceTitle,
    DealerCode,
	DealerName,
    Comment,
    GoodsCode,
    qty,
    CustPrice,
    Amount,
    Dis1,
    Dis2,
    Dis3,
    Add1,
    Add2,
    AmountNut,
    IsConfirm,
    oldcode,
    PrizeType,
    ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) as rownum
INTO #tmp01  
FROM 
(
    -- فروش عادی
    SELECT 
        NULL as TorderNo,
        SaleDate as FreeInvoiceDate,
        22 as DCCode,
        22 as StockDCCode,
        C.CustCode,
        9 as BuyTypeCode,
        N'رسيد' as PaymentUsanceTitle,
        CASE 
            WHEN s.DCName = 'Tehran West' THEN '0005'
            WHEN s.DCName = 'Kerman' THEN '0016'
            WHEN s.DCName = 'Ahvaz' THEN '0010'
            WHEN s.DCName = 'Orumiyeh' THEN '0007'
            WHEN s.DCName = 'Rasht' THEN '0012'
            WHEN s.DCName = 'Hamedan' THEN '0018'
            WHEN s.DCName = 'Mashhad' THEN '0017'
            WHEN s.DCName = 'Babol' THEN '0009'
            WHEN s.DCName = 'Qom' THEN '0014'
            WHEN s.DCName = 'Shiraz' THEN '0013'
            WHEN s.DCName = 'Central Warehouse' THEN '0008'
            WHEN s.DCName = 'Isfahan' THEN '0006'
            WHEN s.DCName = 'Karaj' THEN '0015'
            WHEN s.DCName = 'Tabriz' THEN '0011'
            ELSE NULL 
        END as DealerCode,
		DealerName,
        SaleNo As Comment,
        LTRIM(RTRIM(G.GoodsCode)) as GoodsCode,
        CAST(SaleAndPrizeQty as int) - CAST(prizeqty as int) as qty,
        CustPrice as CustPrice,
        (CAST(SaleAndPrizeQty as int) - CAST(prizeqty as int)) * CustPrice as Amount,
        0 as Dis1,
        Dis2 as Dis2,
        Dis3 as Dis3,
        Add1,
        Add2,
        AmountNut as AmountNut,
        1 as IsConfirm,
        0 as oldcode,
        0 as PrizeType
    FROM Hamian.dbo.Sale S
    INNER JOIN gnr.tblGoods G ON G.PGoodsCode = S.GoodsCode
    INNER JOIN gnr.tblcust C ON C.Comment = S.CustCode
    WHERE saleno <> 0 
        AND SaleIndex = 'SALE'
        AND CAST(SaleAndPrizeQty as int) - CAST(prizeqty as int) > 0
        AND NOT EXISTS (
            SELECT 1 FROM sle.tblFreeInvoiceHdr T 
            WHERE T.Comment = S.SaleNo AND T.CustRef = C.id
        )

    UNION ALL

    -- جایزه
    SELECT 
        NULL as TorderNo,
        SaleDate as FreeInvoiceDate,
        22 as DCCode,
        22 as StockDCCode,
        C.CustCode,
        9 as BuyTypeCode,
        N'رسيد' as PaymentUsanceTitle,
        CASE 
            WHEN s.DCName = 'Tehran West' THEN '0005'
            WHEN s.DCName = 'Kerman' THEN '0016'
            WHEN s.DCName = 'Ahvaz' THEN '0010'
            WHEN s.DCName = 'Orumiyeh' THEN '0007'
            WHEN s.DCName = 'Rasht' THEN '0012'
            WHEN s.DCName = 'Hamedan' THEN '0018'
            WHEN s.DCName = 'Mashhad' THEN '0017'
            WHEN s.DCName = 'Babol' THEN '0009'
            WHEN s.DCName = 'Qom' THEN '0014'
            WHEN s.DCName = 'Shiraz' THEN '0013'
            WHEN s.DCName = 'Central Warehouse' THEN '0008'
            WHEN s.DCName = 'Isfahan' THEN '0006'
            WHEN s.DCName = 'Karaj' THEN '0015'
            WHEN s.DCName = 'Tabriz' THEN '0011'
            ELSE NULL 
        END as DealerCode,
		DealerName,
        SaleNo As Comment,
        LTRIM(RTRIM(G.GoodsCode)) as GoodsCode,
        PrizeQty as qty,
        CustPrice as CustPrice,
        PrizeQty * CustPrice as Amount,
        Dis1 as Dis1,
        0 as Dis2,
        0 as Dis3,
        0 as Add1,
        0 as Add2,
        0 as AmountNut,
        1 as IsConfirm,
        0 as oldcode,
        1 as PrizeType
    FROM Hamian.dbo.Sale S
    INNER JOIN gnr.tblGoods G ON G.PGoodsCode = S.GoodsCode
    INNER JOIN gnr.tblcust C ON C.Comment = S.CustCode
    WHERE saleno <> 0 
        AND SaleIndex = 'SALE'
        AND PrizeQty > 0
        AND NOT EXISTS (
            SELECT 1 FROM sle.tblFreeInvoiceHdr T 
            WHERE T.Comment = S.SaleNo AND T.CustRef = C.id
        )
) n;

-- ادامه با #tmp02
SELECT t.*,
    G.ID as GoodsRef,
    G.UnitRef as GoodsUnitRef,
    D.ID as DCRef,
    SD.ID as StockDCRef,
    SD.DCRef as StockDC_DCRef,
    C.ID as CustRef,
    C.DCRef as Cust_DCRef,
    De.ID as DealerRef,
    De.DCRef as Dealer_DCRef,
    P.ID as BuyTypeId,
    9 as PaymentUsanceRef,
    Cd.SolarDateFull as SolarDateFull,
    Cd.SolarDateFullNoSeperator as SolarDateFullNoSeperator,
    Cd.SolarYear as SolarYear,
    A.AccYear as AccYear,
    O.LastDate as LastDate,
    SG.ID as StockGoodsRef,
    DCS.ID as DCSaleOfficeRef,
    DCS.SaleOfficeRef as SaleOfficeRef,
    DS.SupervisorRef as SupervisorRef,
    t.rownum as rownum2
INTO #tmp02
FROM #tmp01 t
LEFT JOIN gnr.tblGoods G ON t.GoodsCode COLLATE DATABASE_DEFAULT = G.GoodsCode
LEFT JOIN gnr.tblDC D ON t.DCCode = D.DCCode
LEFT JOIN gnr.tblStockDC SD ON t.StockDCCode = SD.StockDCCode
LEFT JOIN gnr.tblCust C ON t.CustCode = C.CustCode
LEFT JOIN gnr.vwDealer De ON t.DealerCode COLLATE DATABASE_DEFAULT = De.PersCode
LEFT JOIN gnr.tblPaymentType P ON t.BuyTypeCode = P.Id
LEFT JOIN gnr.tblPaymentUsance PU ON P.ID = PU.BuyTypeId AND t.PaymentUsanceTitle COLLATE DATABASE_DEFAULT = PU.Title
LEFT JOIN Calendar Cd ON t.FreeInvoiceDate COLLATE DATABASE_DEFAULT = Cd.SolarDateFull
LEFT JOIN gnr.tblAccYear A ON Cd.SolarDateFull BETWEEN A.StartDate AND A.EndDate
LEFT JOIN gnr.tblOprDate O ON A.AccYear = O.AccYear AND D.ID = O.DCRef AND O.SysRef = 1 AND O.IsClosed = 0
LEFT JOIN gnr.tblStockGoods SG ON G.ID = SG.GoodsRef AND SD.ID = SG.StockDCRef AND A.AccYear = SG.AccYear
LEFT JOIN gnr.tblDCSaleOffice DCS ON D.ID = DCS.DCRef AND SD.ID = DCS.StockDcRef
LEFT JOIN gnr.tblDealerSupervisor DS ON De.ID = DS.DealerRef 
    AND (Cd.SolarDateFull BETWEEN DS.StartDate AND ISNULL(DS.Enddate, Cd.SolarDateFull))
WHERE 1=1;

-- ادامه با #tmp03
SELECT 
    DENSE_RANK() OVER(ORDER BY SolarDateFull, Torderno) AS HdrId,
    ROW_NUMBER() OVER(ORDER BY SolarDateFull, Torderno, PrizeType, GoodsCode, AmountNut DESC) AS ItmId,
    DENSE_RANK() OVER(PARTITION BY SaleOfficeRef ORDER BY SolarDateFull, Torderno) AS FreeInvoiceNo,
    ROW_NUMBER() OVER(PARTITION BY Torderno ORDER BY PrizeType, SolarDateFull, Torderno, GoodsCode) AS RowOrder,
    *
INTO #tmp03
FROM #tmp02 t
ORDER BY 1,2;

PRINT 'ایجاد جداول موقت کامل شد';             
                        
 -------     
  
 -------------------------Validation-------------------------            
          
 -------Insert                         
 begin tran            
 IF CURSOR_STATUS('global','crsr') >= 0
BEGIN
    CLOSE crsr
    DEALLOCATE crsr
END
                  
  declare crsr cursor for                              
  select distinct SaleOfficeRef, AccYear, DCRef, IsConfirm  , Comment , CustCode  ,DealerName                
  from #tmp03                        
                
  open crsr                        
  fetch next from crsr into @SaleOfficeRef, @AccYear, @DCRef, @IsConfirm ,@Comment , @CustCode ,@DealerName                     
  while @@FETCH_STATUS=0                              
  begin                              
                        
   IF OBJECT_ID('tempdb.dbo.#tmp04', 'U') IS NOT NULL DROP TABLE #tmp04                        
   IF OBJECT_ID('tempdb.dbo.#tmp05', 'U') IS NOT NULL DROP TABLE #tmp05                        
                        
   set @gtdt = getdate()                        
       
   select @Cnt_Hdr = count(distinct HdrId) from #tmp03 where SaleOfficeRef=@SaleOfficeRef                        
   select @Cnt_Itm = count(distinct ItmId) from #tmp03 where SaleOfficeRef=@SaleOfficeRef AND PrizeType = 0                      
                 
if object_id ('idgen.tblfreeinvoiceitmidgen')  is not null drop table idgen.tblfreeinvoiceitmidgen                       
if object_id ('idgen.tblfreeinvoicehdridgen')  is not null drop table idgen.tblfreeinvoicehdridgen                 
if object_id ('idgen.tblFreeInvoiceHdr_Sequence')  is not null drop sequence idgen.tblFreeInvoiceHdr_Sequence             
if object_id ('idgen.tblFreeInvoiceitm_Sequence')  is not null drop sequence idgen.tblFreeInvoiceitm_Sequence        
if object_id ('idgen.tblOrderHdrCustPathInfo_Sequence')  is not null drop sequence idgen.tblOrderHdrCustPathInfo_Sequence                   
if object_id ('idgen.tblOrderHdrCustPathInfoidgen')  is not null drop table idgen.tblOrderHdrCustPathInfoidgen                        
                     
   exec gnr.uspGetNextId 'sle.tblFreeInvoiceHdr', @NewId_Hdr output, @Cnt_Hdr                        
   exec gnr.uspGetNextId 'sle.tblFreeInvoiceItm', @NewId_Itm output, @Cnt_Itm                        
                        
   exec dbo.GetMaxFreeInvoiceNo @SaleOfficeRef, @AccYear, @FreeInvoiceNo OUTPUT, 0, @DCRef, @Cnt_Hdr         
          
   select t.HdrId+@NewId_Hdr-1 as NewHdrId                        
    , t.ItmId+@NewId_Itm-1 as NewItmId                        
    , t.FreeInvoiceNo+@FreeInvoiceNo-1 as NewFreeInvoiceNo                        
  , *                         
  into #tmp04                        
   from #tmp03 t                        
   where t.SaleOfficeRef=@SaleOfficeRef  and t.Comment = @Comment and t.CustCode = @CustCode


                        
  create clustered index IX_0 on #tmp04 (NewHdrId)                      
  create nonclustered index IX_1 on #tmp04 (NewItmId)                      
  create nonclustered index IX_2 on #tmp04 (SaleOfficeRef)                      
                      
                      
   select t.NewHdrId ,t.custref                       
    , sum(convert(decimal(18,3),t.Qty)) as SumOrderQty                                          
    , sum(convert(money,t.Amount)) as SumAmount                        
    , sum(convert(money,t.Dis1)) as Dis1                        
    , sum(convert(money,t.Dis2)) as Dis2                        
    , sum(convert(money,t.Dis3)) as Dis3                        
    , sum(convert(money,t.Add1)) as Add1                        
    , sum(convert(money,t.Add2)) as Add2                        
                        
   into #tmp05                        
   from #tmp04 t                        
   group by t.NewHdrId,t.custref    

     create clustered index IX_0 on #tmp05 (NewHdrId)                      
                      
   set @Total = @Total + @Cnt_Hdr     
   --
  

  begin try                        
                        
   IF OBJECT_ID('SLE.trg_VN_Replication_tblFreeInvoiceHdr_INSERT', 'TR') IS NOT NULL                      
  ALTER TABLE SLE.tblFreeInvoiceHdr DISABLE TRIGGER trg_VN_Replication_tblFreeInvoiceHdr_INSERT                       
       print '** Start  SLE.tblFreeInvoiceHdr **'
	   
   INSERT INTO SLE.tblFreeInvoiceHdr (                            
       ID, FreeInvoiceNo, FreeInvoiceDate, CustRef, DealerRef, Path, Dis1, Dis2, Dis3, Add1, Add2, CancelFlag                        
       , StockDcRef, DCRef, DCSaleOfficeRef, AccYear, SumFreeInvoiceQty, UserRef, CreateDate, ConfirmUserRef                        
       , ConfirmDate, UpdateUserRef, ChangeDate, Amount, IsNotReturnable, Comment, TOrderNo, CashAmount, PaymentUsanceRef                        
    ,ordertype,FreeInvoiceType            
    )                        
                        
   select distinct t.NewHdrId                        
    ,  t.NewFreeInvoiceNo as FreeInvoiceNo                        
 --, t.TorderNo                      
    , t.SolarDateFull                        
, t.CustRef, t.DealerRef                        
    , 8888 as [Path]                        
    , t5.Dis1, t5.Dis2, t5.Dis3, t5.Add1, t5.Add2                        
    , 0 as CancelFlag                        
   , t.StockDCRef, t.DCRef, t.DCSaleOfficeRef, t.AccYear, t5.SumOrderQty                        
    , 2 as UserRef                        
    , @Today as CreateDate                        
    , 2, @Today                                          
    , 2 as UpdateUserRef                       
    , @Today as ChangeDate                        
    , t5.SumAmount as Amount                                  , 1 as IsNotReturnable                                          
    ,t.Comment As Comment                        
    , t.TorderNo                        
    , 0 as CashAmount                        
    , t.PaymentUsanceRef                        
 ,2,1            
 --select *                        
 from #tmp04 t                         
    inner join #tmp05 t5 on t.NewHdrId=t5.NewHdrId and t.Comment = @Comment and t.CustCode = @CustCode                                 
   where t.SaleOfficeRef=@SaleOfficeRef                        
                print '////End  SLE.tblFreeInvoiceHdr///'             
 IF OBJECT_ID('SLE.trg_VN_Replication_tblFreeInvoiceHdr_INSERT', 'TR') IS NOT NULL                      
 ALTER TABLE SLE.tblFreeInvoiceHdr ENABLE TRIGGER trg_VN_Replication_tblFreeInvoiceHdr_INSERT                       
                       
 IF OBJECT_ID('SLE.trg_VN_Replication_tblFreeInvoiceItm_INSERT', 'TR') IS NOT NULL                      
 ALTER TABLE SLE.tblFreeInvoiceItm Disable TRIGGER trg_VN_Replication_tblFreeInvoiceItm_INSERT                       
                  print '** Start  SLE.tblFreeInvoiceItm *** '         
   -- در بخش INSERT INTO SLE.tblFreeInvoiceItm
print '** Start  SLE.tblFreeInvoiceItm *** '         
INSERT INTO SLE.tblFreeInvoiceItm (                            
   ID, HdrRef, RowOrder, GoodsRef, UnitRef, UnitCapacity, UnitQty, TotalQty, Amount, DiscountPercent                        
   , Discount, AmountNut, AccYear, PrizeType, SupAmount, AddAmount, CustPrice, CPriceRef, IsDeleted                        
   , MainQty, Dis1, Dis2, Dis3, Add1, Add2, AddAmountPercent                        
)                          
select t.NewItmId                        
  , t.NewHdrId                        
  , t.RowOrder                        
  , t.GoodsRef, t.GoodsUnitRef, 1 as UnitCapacity, convert(decimal(18,3),t.Qty) as UnitQty, convert(decimal(18,3),t.Qty) as TotalQty, 
  convert(money,t.Amount) as Amount                     
  , case 
      when t.PrizeType = 1 then 100  -- برای جایزه DiscountPercent = 100
      when convert(money,t.Amount) <> 0 then ((convert(money,t.Dis1)+convert(money,t.Dis2)+convert(money,t.Dis3))/convert(money,t.Amount))*100 
      else 0 
    end as DiscountPercent                        
  , CASE 
      WHEN t.PrizeType = 1 THEN convert(money,t.Amount)  -- برای جایزه Discount = Amount
      ELSE (convert(money,t.Dis1)+convert(money,t.Dis2)+convert(money,t.Dis3))
    END as Discount                        
  , CASE 
      WHEN t.PrizeType = 1 THEN 0  -- برای جایزه AmountNut = 0
      ELSE ((convert(money,t.Amount))-(convert(money,t.Dis1)+convert(money,t.Dis2)+convert(money,t.Dis3))+(convert(money,t.Add1)+convert(money,t.Add2)))
    END as AmountNut                        
  , t.AccYear as AccYear                        
  , t.PrizeType, null as SupAmount                                      
  , CASE 
      WHEN t.PrizeType = 1 THEN 0  -- برای جایزه AddAmount = 0
      ELSE (convert(money,t.Add1)+convert(money,t.Add2))
    END as AddAmount                        
  -- CustPrice: برای جایزه باید برابر با Amount/TotalQty باشد، برای عادی از CustPrice اصلی استفاده شود
  , CASE 
      WHEN t.PrizeType = 1 AND t.qty > 0 THEN convert(money,t.Amount) / convert(decimal(18,3),t.Qty)
      ELSE convert(money,t.CustPrice)  -- استفاده از CustPrice اصلی برای کالاهای عادی
    END as CustPrice                    
  , null as CPriceRef, 0 as IsDeleted, convert(decimal(18,3),t.Qty) as MainQty                        
  , CASE 
      WHEN t.PrizeType = 1 THEN 0  -- برای جایزه Dis1 = 0
      ELSE convert(money,t.Dis1)
    END as Dis1                        
  , CASE 
      WHEN t.PrizeType = 1 THEN 0  -- برای جایزه Dis2 = 0
      ELSE convert(money,t.Dis2)
    END as Dis2                        
  , CASE 
      WHEN t.PrizeType = 1 THEN convert(money,t.Amount)  -- برای جایزه Dis3 = Amount
      ELSE convert(money,t.Dis3)
    END as Dis3                        
  , CASE 
      WHEN t.PrizeType = 1 THEN 0  -- برای جایزه Add1 = 0
      ELSE convert(money,t.Add1)
    END as Add1                        
  , CASE 
      WHEN t.PrizeType = 1 THEN 0  -- برای جایزه Add2 = 0
      ELSE convert(money,t.Add2)
    END as Add2                        
  , case when ((convert(money,t.Amount))-(convert(money,t.Dis1)+convert(money,t.Dis2)+convert(money,t.Dis3))) <> 0                        
     then (convert(money,t.Add1)+convert(money,t.Add2))/((convert(money,t.Amount))-(convert(money,t.Dis1)+convert(money,t.Dis2)+convert(money,t.Dis3)))                        
     else 0 end as AddAmountPercent                        
from #tmp04 t                         
where t.SaleOfficeRef=1 
                 
                      print '/// End  SLE.tblFreeInvoiceItm ///' 
  IF OBJECT_ID('SLE.trg_VN_Replication_tblFreeInvoiceItm_INSERT', 'TR') IS NOT NULL                      
 ALTER TABLE SLE.tblFreeInvoiceItm Enable TRIGGER trg_VN_Replication_tblFreeInvoiceItm_INSERT                       
  ---------------------------------------------------------------                        
  ---------------------------------------------------------------                        
    
    -----    
 if object_id ('idgen.tblfreeinvoiceitmidgen')  is not null drop table idgen.tblfreeinvoiceitmidgen                                         
if object_id ('idgen.tblfreeinvoicehdridgen')  is not null drop table idgen.tblfreeinvoicehdridgen                    
     
 IF @IsConfirm=1                      
  BEGIN                        
   DECLARE @NewOrderId_Hdr int, @OrderNo int                      
   DECLARE @NewOrderId_Itm int                      
                        
     declare @idnew int    
  EXEC gnr.uspGetNextId 'sle.tblSaleHdrDetail',@idnew output    
        EXEC gnr.uspGetNextId 'sle.tblOrderHdrCustPathInfo',@idnew output    
   EXEC GNR.uspGetNextId 'SLE.tblOrderHdr', @NewOrderId_Hdr output, @Cnt_Hdr                          
   EXEC dbo.GetMaxOrderNo @OrderNo=@OrderNo output, @SaleOfficeRef=@SaleOfficeRef, @AccYear=@AccYear, @NoSelect=1, @DcRef=@DCRef, @Count = @Cnt_Hdr                       
                      
   EXEC GNR.uspGetNextId 'SLE.tblOrderItm', @NewOrderId_Itm output, @Cnt_Itm          
                       
   IF OBJECT_ID('tempdb.dbo.#OrderHdrID', 'U') IS NOT NULL                      
  DROP TABLE #OrderHdrID                      
                   
   CREATE TABLE #OrderHdrID (ID INT)                      
                      
   IF OBJECT_ID('SLE.trg_VN_Replication_tblOrderHdr_INSERT', 'TR') IS NOT NULL                      
  ALTER TABLE SLE.tblOrderHdr DISABLE TRIGGER trg_VN_Replication_tblOrderHdr_INSERT                      
                          print ' ** Start  SLE.tblOrderHdr **'             
   INSERT INTO SLE.tblOrderHdr(ID,StockDCRef,DealerRef,CustRef,OrderNo,OrderDate,Dis1,Dis2,Dis3,Add1,Add2,SumOrderQty,ChangeDate,UserRef,                        
   ConfirmDate,AccYear,DCRef,Path,OrderType,DisType,PaymentUsanceRef, DCSaleOfficeRef,                      
   CreateDate,ConfirmUserRef,UpdateUserRef, CashAmount,IsNotReturnable,Comment,TOrderNo,FreeInvoiceHdrRef , ShipDate,CreationBy,ModifiedBy,MainDealerRef )                         
   OUTPUT inserted.ID INTO #OrderHdrID                      
   SELECT ROW_NUMBER() OVER(ORDER BY ID) + @NewOrderId_Hdr - 1 AS ID, StockDcRef, DealerRef, CustRef, ROW_NUMBER() OVER(ORDER BY ID) + @OrderNo - 1 AS ORderNo, FreeInvoiceDate AS OrderDate, Dis1, Dis2,                       
   CASE WHEN Amount = 0 THEN 0 ELSE (Dis3 * 100) / Amount END AS Dis3,                       
   CASE WHEN (Amount - (Dis1 + Dis2 + Dis3)) <> 0 THEN (Add1 * 100) / (Amount - (Dis1 + Dis2 + Dis3)) ELSE 0 END AS Add1,      
   CASE WHEN (Amount - (Dis1 + Dis2 + Dis3)) <> 0 THEN (Add2 * 100) / (Amount - (Dis1 + Dis2 + Dis3)) ELSE 0 END AS Add2,                       
   SumFreeInvoiceQty AS SumOrderQty, ChangeDate, UserRef,                      
      ConfirmDate, AccYear, DCRef, Path, 1001 AS Ordertype, 3 AS DisType, PaymentUsanceRef, DCSaleOfficeRef,                      
      CreateDate, ConfirmUserRef, UpdateUserRef, CashAmount, IsNotReturnable, Comment, TOrderNo, ID AS FreeInvoiceHdrRef, FreeInvoiceDate AS ShipDate, UserRef AS CreationBy, UserRef AS ModifiedBy ,
	  (select top 1 D.ID from GNR.vwDealer D where D.FullName = @DealerName) as MainDealerRef
   FROM SLE.tblFreeInvoiceHdr                      
   WHERE ID IN (SELECT NewHdrId FROM #tmp04)                            
           print '///End  SLE.tblOrderHdr/// '        

   IF OBJECT_ID('SLE.trg_VN_Replication_tblOrderItm_INSERT', 'TR') IS NOT NULL                      
  ALTER TABLE SLE.tblOrderItm DISABLE TRIGGER trg_VN_Replication_tblOrderItm_INSERT                      
         PRINT ' ** Start  SLE.tblOrderItm ** '            
INSERT INTO SLE.tblOrderItm(ID,HdrRef,RowOrder,GoodsRef,OrderQty,SoldQty,CPriceRef,AccYear,UnitRef, IsUsed,MainQty, FreeReasonId)                        
SELECT ROW_NUMBER() OVER(ORDER BY FI.ID) + @NewOrderId_Itm - 1 AS ID                      
   , OH.ID AS HdrRef
   , ROW_NUMBER() OVER(PARTITION BY OH.ID ORDER BY 
        FI.GoodsRef,
        FI.ID
     ) AS RowOrder                 
  , FI.GoodsRef                      
  , FI.TotalQty                      
  , 0 AS SoldQty                      
  , null AS CPriceRef                      
  , FI.AccYear                      
  , FI.UnitRef                      
  , 0 AS IsUsed                      
  , FI.TotalQty AS MainQty
  , NULL AS FreeReasonId
FROM SLE.tblFreeInvoiceItm FI 
     INNER JOIN SLE.tblOrderHdr OH ON FI.HdrRef = OH.FreeInvoiceHdrRef                      
     INNER JOIN GNR.tblPaymentUsance PU ON OH.PaymentUsanceRef = PU.ID                                      
WHERE FI.ID IN (SELECT NewItmId FROM #tmp04)
  AND FI.PrizeType = 0
        print '/// End  SLE.tblOrderItm/// '     
   IF OBJECT_ID('SLE.trg_VN_Replication_tblOrderItm_INSERT', 'TR') IS NOT NULL                      
      ALTER TABLE SLE.tblOrderItm ENABLE TRIGGER trg_VN_Replication_tblOrderItm_INSERT                      
   -------------------                      
                        
  DECLARE @SaleNo INT, @SaleVocherNo INT, @NewSaleId_Hdr INT                      
  DECLARE @NewSaleId_Itm INT      
                         
   EXEC dbo.GetMaxSaleVocherNo @SaleOfficeRef, @AccYear, @SaleVocherNo output, 0, @DCRef                      
   EXEC dbo.GetMaxSaleNo @SaleOfficeRef, @AccYear, @SaleNo output, 0, @DCRef                        
                         
   EXEC GNR.uspGetNextId 'SLE.tblSalehdr', @NewSaleId_Hdr output, @Cnt_Hdr                    
   EXEC GNR.uspGetNextId 'SLE.tblSaleItm', @NewSaleId_Itm output, @Cnt_Itm                      
                         
   IF OBJECT_ID('SLE.trg_VN_Replication_tblSaleHdr_INSERT', 'TR') IS NOT NULL                      
  ALTER TABLE SLE.tblSaleHdr DISABLE TRIGGER trg_VN_Replication_tblSaleHdr_INSERT                      
                       print 'Start  SLE.tblSaleHdr '
   INSERT INTO SLE.tblSalehdr(ID,SaleNo,SaleVocherNo,OrderRef,SaleDate,   Dis1,Dis2,Dis3,Add1,Add2,   TotalAmount,                        
   UserRef,PaymentUsanceRef,   AccYear,DCRef,StockDCRef,CustRef,DealerRef,   CancelFlag,OrderType,DisType,DCSaleOfficeRef,Status,ChangeDate)                       
   SELECT ROW_NUMBER() OVER(ORDER BY ID) + @NewSaleId_Hdr - 1 AS ID, SaleNo,SaleVocherNo,OrderRef,SaleDate,   
   round(Dis1, 0) as Dis1,   -- گرد کردن به عدد صحیح
   round(Dis2, 0) as Dis2,   -- گرد کردن به عدد صحیح
   round(Dis3, 0) as Dis3,   -- گرد کردن به عدد صحیح
   round(Add1, 0) as Add1,   -- گرد کردن به عدد صحیح
   round(Add2, 0) as Add2,   -- گرد کردن به عدد صحیح
   round(TotalAmount, 0) as TotalAmount,   -- گرد کردن به عدد صحیح
   UserRef,PaymentUsanceRef,   AccYear,DCRef,StockDCRef,CustRef,DealerRef,   CancelFlag,OrderType,DisType,DCSaleOfficeRef,Status,ChangeDate                      
   FROM (SELECT FH.ID, NULL AS SaleNo, ROW_NUMBER() OVER(ORDER BY FH.ID) + @SaleVocherNo - 1 AS SaleVocherNo, OH.ID AS OrderRef,            
   FH.FreeInvoiceDate AS SaleDate, 
   round(FH.Dis1, 0) as Dis1, 
   round(FH.Dis2, 0) as Dis2, 
   round(FH.Dis3, 0) as Dis3, 
   round(FH.Add1, 0) as Add1, 
   round(FH.Add2, 0) as Add2,
   round(FH.Amount - (FH.Dis1 + FH.Dis2 + FH.Dis3) + (FH.Add1 + FH.Add2), 0) AS TotalAmount,                      
     FH.UserRef, FH.PaymentUsanceRef, FH.AccYear, FH.DCRef, FH.StockDcRef, FH.CustRef, FH.DealerRef, FH.CancelFlag, OH.OrderType, OH.DisType, FH.DCSaleOfficeRef, 2 AS Status, OH.ChangeDate                      
     FROM SLE.tblFreeInvoiceHdr FH INNER JOIN SLE.tblOrderHdr OH ON FH.ID = OH.FreeInvoiceHdrRef                      
     WHERE FH.ID IN (SELECT NewHdrId FROM #tmp04)                      
     AND EXISTS (SELECT 1 FROM gnr.tblServerConfig WHERE KeyName = 'FreeFactorIssueMethod' AND KeyValue=2)                      
     AND EXISTS (SELECT 1 FROM gnr.tblCust WHERE Id = FH.CustRef AND CustType=1)                      
                         
     UNION ALL                      
                         
   SELECT FH.ID, ROW_NUMBER() OVER(ORDER BY FH.ID) + @SaleNo - 1 AS SaleNo, NULL AS SaleVocherNo, OH.ID AS OrderRef, FH.FreeInvoiceDate AS SaleDate, 
   round(FH.Dis1, 0) as Dis1, 
   round(FH.Dis2, 0) as Dis2, 
   round(FH.Dis3, 0) as Dis3, 
   round(FH.Add1, 0) as Add1, 
   round(FH.Add2, 0) as Add2,
   round(FH.Amount - (FH.Dis1 + FH.Dis2 + FH.Dis3) + (FH.Add1 + FH.Add2), 0) AS TotalAmount,                      
     FH.UserRef, FH.PaymentUsanceRef, FH.AccYear, FH.DCRef, FH.StockDcRef, FH.CustRef, FH.DealerRef, FH.CancelFlag, OH.OrderType, OH.DisType, FH.DCSaleOfficeRef, 1 AS Status, OH.ChangeDate                      
     FROM SLE.tblFreeInvoiceHdr FH INNER JOIN SLE.tblOrderHdr OH ON FH.ID = OH.FreeInvoiceHdrRef                      
     WHERE FH.ID IN (SELECT NewHdrId FROM #tmp04)                      
     AND (NOT EXISTS (SELECT 1 FROM gnr.tblServerConfig WHERE KeyName = 'FreeFactorIssueMethod' AND KeyValue=2)                      
      OR NOT EXISTS (SELECT 1 FROM gnr.tblCust WHERE Id = FH.CustRef AND CustType=1)                       
      )                      
     ) T         
                        print 'End  SLE.tblSaleHdr '        
   IF OBJECT_ID('SLE.trg_VN_Replication_tblSaleHdr_INSERT', 'TR') IS NOT NULL                      
  ALTER TABLE SLE.tblSaleHdr ENABLE TRIGGER trg_VN_Replication_tblSaleHdr_INSERT                         
                         
   UPDATE OH SET SaleHdrRef = SH.ID                      
   FROM SLE.tblOrderHdr OH INNER JOIN SLE.tblSaleHdr SH ON OH.ID = SH.OrderRef                      
   WHERE FreeInvoiceHdrRef IN (SELECT NewHdrId FROM #tmp04)                      
                           
   IF OBJECT_ID('SLE.trg_VN_Replication_tblOrderHdr_INSERT', 'TR') IS NOT NULL                      
   ALTER TABLE SLE.tblOrderHdr ENABLE TRIGGER trg_VN_Replication_tblOrderHdr_INSERT                      
                            
   IF OBJECT_ID('SLE.trg_VN_Replication_tblSaleItm_INSERT', 'TR') IS NOT NULL                      
     ALTER TABLE SLE.tblSaleItm DISABLE TRIGGER trg_VN_Replication_tblSaleItm_INSERT                      
     
--PRINT 'بررسی مقادیر برای constraint Discount:'
--SELECT 
--    FI.ID,
--    FI.PrizeType,
--    FI.Dis1,
--    FI.Dis2,
--    FI.Dis3,
--    FI.Discount as Original_Discount,
--    FI.Dis1 + FI.Dis2 + FI.Dis3 as Calculated_Discount,
--    CASE 
--        WHEN FI.PrizeType = 1 AND FI.Discount = FI.Amount THEN 'OK (جایزه)'
--        WHEN FI.PrizeType = 0 AND FI.Discount = FI.Dis1 + FI.Dis2 + FI.Dis3 THEN 'OK (عادی)'
--        ELSE 'خطا'
--    END as Check_Result
--FROM SLE.tblFreeInvoiceItm FI 
--WHERE FI.ID IN (SELECT NewItmId FROM #tmp04)

PRINT 'Start  SLE.tblSaleitm '
-- غیرفعال کردن موقت constraints
--ALTER TABLE SLE.tblSaleItm NOCHECK CONSTRAINT CK_tblSaleItm_Amount
--ALTER TABLE SLE.tblSaleItm NOCHECK CONSTRAINT CK_tblSaleItm_AmountNut
--ALTER TABLE SLE.tblSaleItm NOCHECK CONSTRAINT CK_tblSaleItm_Discount
--ALTER TABLE SLE.tblSaleItm NOCHECK CONSTRAINT ALL
print @Comment

INSERT INTO SLE.tblSaleitm (ID, HdrRef, RowOrder, GoodsRef, UnitRef, UnitCapasity, UnitQty, TotalQty,                         
    AmountNut, Discount, CustPrice, Amount, UserPrice, AccYear, PrizeType, SupAmount,                      
    AddAmount,PriceRef, CPriceRef, IsDeleted, MainQty, Dis1, Dis2, Dis3, Add1, Add2, OtherDiscount, OtherAddition)                         
SELECT  
    ROW_NUMBER() OVER(ORDER BY FI.ID) + @NewSaleId_Itm - 1 AS ID, 
    OH.SaleHdrRef, 
    ROW_NUMBER() OVER(PARTITION BY OH.SaleHdrRef ORDER BY 
        FI.PrizeType,
        FI.GoodsRef,
        FI.ID
    ) AS RowOrder,    
    FI.GoodsRef, 
    FI.UnitRef, 
    FI.UnitCapacity, 
    FI.UnitQty, 
    FI.TotalQty,                         
    round(FI.AmountNut, 0) AS AmountNut, 
    -- Discount باید برابر با مجموع Dis1 + Dis2 + Dis3 باشد
    CASE 
        WHEN FI.PrizeType = 1 THEN round(FI.Amount, 0)
        ELSE round(FI.Dis1 + FI.Dis2 + FI.Dis3, 0)  -- مجموع Disها
    END AS Discount, 
    FI.CustPrice AS CustPrice,
    round(FI.Amount, 0) AS Amount, 
    0 AS UserPrice, 
    FI.AccYear, 
    FI.PrizeType, 
    FI.SupAmount,                         
    CASE 
        WHEN FI.PrizeType = 1 THEN 0
        ELSE FI.AddAmount
    END AS AddAmount, 
    NULL AS PriceRef, 
    FI.CPriceRef, 
    FI.IsDeleted, 
    FI.MainQty, 
    CASE 
        WHEN FI.PrizeType = 1 THEN 0
        ELSE FI.Dis1
    END AS Dis1, 
    CASE 
        WHEN FI.PrizeType = 1 THEN 0
        ELSE FI.Dis2
    END AS Dis2, 
    CASE 
        WHEN FI.PrizeType = 1 THEN round(FI.Amount, 0)
        ELSE FI.Dis3
    END AS Dis3, 
    CASE 
        WHEN FI.PrizeType = 1 THEN 0
        ELSE FI.Add1
    END AS Add1, 
    CASE 
        WHEN FI.PrizeType = 1 THEN 0
        ELSE FI.Add2
    END AS Add2,
    0 AS OtherDiscount,  -- همیشه صفر
    0 AS OtherAddition   -- همیشه صفر
FROM SLE.tblFreeInvoiceItm FI 
INNER JOIN SLE.tblOrderHdr OH ON FI.HdrRef = OH.FreeInvoiceHdrRef 
inner join gnr.tblCust C on C.id = OH.CustRef
WHERE FI.ID IN (SELECT NewItmId FROM #tmp04)

-- فعال کردن مجدد constraints
--ALTER TABLE SLE.tblSaleItm CHECK CONSTRAINT ALL
--ALTER TABLE SLE.tblSaleItm CHECK CONSTRAINT CK_tblSaleItm_Amount
--ALTER TABLE SLE.tblSaleItm CHECK CONSTRAINT CK_tblSaleItm_AmountNut
--ALTER TABLE SLE.tblSaleItm CHECK CONSTRAINT CK_tblSaleItm_Discount
          print 'End  SLE.tblSaleitm '           
   IF OBJECT_ID('SLE.trg_VN_Replication_tblSaleItm_INSERT', 'TR') IS NOT NULL                      
     ALTER TABLE SLE.tblSaleItm ENABLE TRIGGER trg_VN_Replication_tblSaleItm_INSERT                         
    exec usp_GenDataFast 'SLE.tblFreeInvoiceHdr', 'id in (select NewHdrId from #tmp04)', 1, 1                        
  exec usp_GenDataFast 'SLE.tblFreeInvoiceItm', 'id in (select NewItmId from #tmp04)', 1, 1                      
                        
  exec dbo.usp_GenDataFast 'SLE.tblOrderHdr', 'id in (select id from #OrderHdrID)', 1, 1                      
  exec dbo.usp_GenDataFast 'SLE.tblOrderItm', 'HdrRef in (select id from #OrderHdrID)', 1, 1                      
                      
  exec dbo.usp_GenDataFast 'SLE.tblSaleHdr', 'OrderRef in (select id from #OrderHdrID)', 1, 1                      
  exec dbo.usp_GenDataFast 'SLE.tblSaleItm', 'HdrRef IN (SELECT ID FROM SLE.tblSaleHdr WHERE OrderRef in (select id from #OrderHdrID))', 1, 1                       
                      
  delete tblSaleNo where SaleOfficeRef = @SaleOfficeRef and AccYear = @AccYear and DcRef = @DCRef --and OrderTypeSerialType is null                      
  delete tblSaleVocherNo where SaleOfficeRef = @SaleOfficeRef and AccYear = @AccYear and DcRef = @DCRef --and OrderTypeSerialType is null                      
                      
  END                
  ---------------------------------------------------------------                        
  ---------------------------------------------------------------                        
                        
   end try                              
   begin catch                              
                        
    close crsr                              
    deallocate crsr                              
                        
    rollback                              
                              
    select @ErrStr = 'خطا در ثبت درخواست و فاکتور'  + char(13)+char(10)+ERROR_MESSAGE()                               
                        
    raiserror (@ErrStr, 16, 1)                              
    return                              
   end CATCH                            
                        
                 
  fetch next from crsr into @SaleOfficeRef, @AccYear, @DCRef, @IsConfirm ,@Comment ,@CustCode ,@DealerName                    
  end                              
  close crsr                              
  deallocate crsr                        
        
  commit tran                               
if object_id ('idgen.tblfreeinvoiceitmidgen')  is not null drop table idgen.tblfreeinvoiceitmidgen                       
if object_id ('idgen.tblfreeinvoicehdridgen')  is not null drop table idgen.tblfreeinvoicehdridgen                       
if object_id ('idgen.tblOrderHdrCustPathInfoidgen')  is not null drop table idgen.tblOrderHdrCustPathInfoidgen                       
                      
update tblFreeInvoiceNo set FreeInvoiceNo=(select MAX(sle.tblFreeInvoiceHdr.FreeInvoiceNo) from sle.tblFreeInvoiceHdr        
 where AccYear=@AccYear and DCRef=@DCRef)where AccYear=@AccYear and DCRef=@DCRef        
                    
 go   
	enable trigger [TRG_VN_LockCN_tblOrderHdr]	on [SLE].[tblOrderHdr]	 
go
enable trigger [TRG_VN_LockCN_tblSaleHdr] on [SLE].[tblSaleHdr]
go 
enable trigger [trg_VN_Replication_tblorderhdr_UPDATE] on [SLE].[tblOrderHdr]
go
enable trigger [Trg_tblSaleItm_UpdateStockGoods] on [SLE].[tblSaleItm]              
            ";

            await ExecuteSqlScript(query2);
        }



        // 🆕 متد عمومی برای اجرای اسکریپت SQL
        // 🆕 متد عمومی برای اجرای اسکریپت SQL (روی دیتابیس مقصد)
        public async Task ExecuteSqlScript(string script)
        {
            using (SqlConnection connection = new SqlConnection(_targetConnectionString))
            {
                await connection.OpenAsync();

                // 🆕 اصلاح split کردن دستورات GO - استفاده از Regex برای تشخیص بهتر
                string[] scriptBatches = System.Text.RegularExpressions.Regex.Split(
                    script,
                    @"^\s*GO\s*$",
                    System.Text.RegularExpressions.RegexOptions.Multiline | System.Text.RegularExpressions.RegexOptions.IgnoreCase
                );

                foreach (string batch in scriptBatches)
                {
                    if (!string.IsNullOrWhiteSpace(batch.Trim()))
                    {
                        using (SqlCommand command = new SqlCommand(batch, connection))
                        {
                            command.CommandTimeout = 3600;
                            try
                            {
                                int rowsAffected = await command.ExecuteNonQueryAsync();
                                Console.WriteLine($"Rows affected: {rowsAffected}");
                            }
                            catch (SqlException ex)
                            {
                                Console.WriteLine($"SQL Error: {ex.Message}");
                                throw;
                            }
                        }
                    }
                }
            }
        }

        // 🆕 متد برای اجرای کوئری و بازگشت نتیجه
        // 🆕 متد برای اجرای کوئری و بازگشت نتیجه (روی دیتابیس مقصد)
        public async Task<DataTable> ExecuteQuery(string query)
        {
            using (var connection = new SqlConnection(_targetConnectionString))
            {
                await connection.OpenAsync();
                using (var command = new SqlCommand(query, connection))
                {
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        var dataTable = new DataTable();
                        dataTable.Load(reader);
                        return dataTable;
                    }
                }
            }
        }

        // 🆕 متد برای اجرای کوئری و بازگشت تعداد رکوردهای تأثیرپذیرفته
        // 🆕 متد برای اجرای کوئری و بازگشت تعداد رکوردهای تأثیرپذیرفته (روی دیتابیس مقصد)
        public async Task<int> ExecuteNonQuery(string query)
        {
            using (var connection = new SqlConnection(_targetConnectionString))
            {
                await connection.OpenAsync();
                using (var command = new SqlCommand(query, connection))
                {
                    return await command.ExecuteNonQueryAsync();
                }
            }
        }

        // متدهای موجود شما (بدون تغییر)
        public async Task<int> SaveSalesData(List<SaleInfoDetail> sales)
        {
            using (var connection = new SqlConnection(_sourceConnectionString))
            {
                await connection.OpenAsync();

                var table = new DataTable();

                // تعریف ستون‌ها با نوع داده متناظر مدل
                table.Columns.Add("Id", typeof(int));
                table.Columns.Add("SaleNo", typeof(int));
                table.Columns.Add("SaleDate", typeof(string)).MaxLength = 20;
                table.Columns.Add("SaleVocherNo", typeof(int));
                table.Columns.Add("SaleVocherDate", typeof(string)).MaxLength = 10;
                table.Columns.Add("CustCode", typeof(string)).MaxLength = 50;
                table.Columns.Add("CustomerName", typeof(string)).MaxLength = 100;
                table.Columns.Add("StoreName", typeof(string)).MaxLength = 100;
                table.Columns.Add("KOB", typeof(string)).MaxLength = 100;
                table.Columns.Add("DealerName", typeof(string)).MaxLength = 100;
                table.Columns.Add("DealerCode", typeof(string)).MaxLength = 50;
                table.Columns.Add("DCName", typeof(string)).MaxLength = 100;
                table.Columns.Add("BrandName", typeof(string)).MaxLength = 100;
                table.Columns.Add("Category", typeof(string)).MaxLength = 100;
                table.Columns.Add("GoodsCode", typeof(string)).MaxLength = 50;
                table.Columns.Add("GoodsName", typeof(string)).MaxLength = 200;
                table.Columns.Add("Barcode", typeof(string)).MaxLength = 50;
                table.Columns.Add("Barcode2", typeof(string)).MaxLength = 50;
                table.Columns.Add("SaleIndex", typeof(string)).MaxLength = 50;
                table.Columns.Add("CartonType", typeof(float));
                table.Columns.Add("SaleAndPrizeCartonQty", typeof(float));
                table.Columns.Add("SaleAndPrizeQty", typeof(float));
                table.Columns.Add("PrizeCartonQty", typeof(float));
                table.Columns.Add("PrizeQty", typeof(float));
                table.Columns.Add("GoodsWeight", typeof(float));
                table.Columns.Add("CustPrice", typeof(float));
                table.Columns.Add("Amount", typeof(float));
                table.Columns.Add("Discount", typeof(float));
                table.Columns.Add("AddAmount", typeof(float));
                table.Columns.Add("AmountMinusDiscount", typeof(float));
                table.Columns.Add("AmountNut", typeof(float));
                table.Columns.Add("PaymentTypeName", typeof(string)).MaxLength = 50;
                table.Columns.Add("Dis1", typeof(float));
                table.Columns.Add("Dis2", typeof(float));
                table.Columns.Add("Dis3", typeof(float));
                table.Columns.Add("Add1", typeof(float));
                table.Columns.Add("Add2", typeof(float));
                table.Columns.Add("SupervisorName", typeof(string)).MaxLength = 100;
                table.Columns.Add("SupervisorCode", typeof(string)).MaxLength = 50;
                table.Columns.Add("AreaName", typeof(string)).MaxLength = 100;
                table.Columns.Add("StateName", typeof(string)).MaxLength = 100;
                table.Columns.Add("AccYear", typeof(int));
                table.Columns.Add("SolarMonthId", typeof(int));
                table.Columns.Add("SolarStrMonth", typeof(string)).MaxLength = 20;
                table.Columns.Add("SolarDay", typeof(string)).MaxLength = 10;
                table.Columns.Add("SolarYearMonth", typeof(string)).MaxLength = 20;
                table.Columns.Add("YearMonth", typeof(string)).MaxLength = 10;

                // پر کردن DataTable
                foreach (var dataObject in sales)
                {
                    table.Rows.Add(
                        dataObject.Id,
                        dataObject.SaleNo ?? (object)DBNull.Value,
                        dataObject.SaleDate ?? (object)DBNull.Value,
                        dataObject.SaleVocherNo ?? (object)DBNull.Value,
                        dataObject.SaleVocherDate ?? (object)DBNull.Value,
                        dataObject.CustCode ?? (object)DBNull.Value,
                        TruncateString(dataObject.CustomerName, 100) ?? (object)DBNull.Value,
                        TruncateString(dataObject.StoreName, 100) ?? (object)DBNull.Value,
                        TruncateString(dataObject.KOB, 100) ?? (object)DBNull.Value,
                        TruncateString(dataObject.DealerName, 100) ?? (object)DBNull.Value,
                        TruncateString(dataObject.DealerCode, 50) ?? (object)DBNull.Value,
                        TruncateString(dataObject.DCName, 100) ?? (object)DBNull.Value,
                        TruncateString(dataObject.BrandName, 100) ?? (object)DBNull.Value,
                        TruncateString(dataObject.Category, 100) ?? (object)DBNull.Value,
                        TruncateString(dataObject.GoodsCode, 50) ?? (object)DBNull.Value,
                        TruncateString(dataObject.GoodsName, 200) ?? (object)DBNull.Value,
                        TruncateString(dataObject.Barcode, 50) ?? (object)DBNull.Value,
                        TruncateString(dataObject.Barcode2, 50) ?? (object)DBNull.Value,
                        TruncateString(dataObject.SaleIndex, 50) ?? (object)DBNull.Value,
                        dataObject.CartonType,
                        dataObject.SaleAndPrizeCartonQty,
                        dataObject.SaleAndPrizeQty,
                        dataObject.PrizeCartonQty,
                        dataObject.PrizeQty,
                        dataObject.GoodsWeight,
                        dataObject.CustPrice,
                        dataObject.Amount,
                        dataObject.Discount,
                        dataObject.AddAmount,
                        dataObject.AmountMinusDiscount,
                        dataObject.AmountNut,
                        TruncateString(dataObject.PaymentTypeName, 50) ?? (object)DBNull.Value,
                        dataObject.Dis1,
                        dataObject.Dis2,
                        dataObject.Dis3,
                        dataObject.Add1,
                        dataObject.Add2,
                        TruncateString(dataObject.SupervisorName, 100) ?? (object)DBNull.Value,
                        TruncateString(dataObject.SupervisorCode, 50) ?? (object)DBNull.Value,
                        TruncateString(dataObject.AreaName, 100) ?? (object)DBNull.Value,
                        TruncateString(dataObject.StateName, 100) ?? (object)DBNull.Value,
                        dataObject.AccYear,
                        dataObject.SolarMonthId,
                        dataObject.SolarStrMonth ?? (object)DBNull.Value,
                        dataObject.SolarDay ?? (object)DBNull.Value,
                        dataObject.SolarYearMonth ?? (object)DBNull.Value,
                        dataObject.YearMonth ?? (object)DBNull.Value
                    );
                }

                // استفاده از SqlBulkCopy
                using (var bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = "dbo.Sale";
                    bulkCopy.BatchSize = 5000;
                    bulkCopy.BulkCopyTimeout = 600;

                    await bulkCopy.WriteToServerAsync(table);
                }

                return sales.Count;
            }
        }

        private string TruncateString(string value, int maxLength)
        {
            return string.IsNullOrEmpty(value) ? value : value.Length <= maxLength ? value : value.Substring(0, maxLength);
        }

        public async Task<int> SaveReturnInfo(List<SaleVoucherReturnInfo> returnInfos)
        {
            int recordCount = 0;

            using (var connection = new SqlConnection(_sourceConnectionString))
            {
                await connection.OpenAsync();

                foreach (var item in returnInfos)
                {
                    var query = @"
            INSERT INTO dbo.SaleReturns (
                CustCode,
                SaleVocherNo,
                DealerCode,
                DealerName,
                SupervisorCode,
                SupervisorName,
                GoodsCode,
                GoodsName,
                TotalAmountRetVocher,
                DiscountRetVocher,
                Dis1RetVocher,
                Dis2RetVocher,
                Dis3RetVocher,
                AddAmountRetVocher,
                Add1RetVocher,
                Add2RetVocher,
                TotalAmountNutRetVocher,
                RetVocherQty,
                RetPrizeVocherQty,
                VocherQty,
                PrizevocherQty,
                PaymentUsanceName,
                RetCauseName,
                ImportDate
            ) VALUES (
                @CustCode,
                @SaleVocherNo,
                @DealerCode,
                @DealerName,
                @SupervisorCode,
                @SupervisorName,
                @GoodsCode,
                @GoodsName,
                @TotalAmountRetVocher,
                @DiscountRetVocher,
                @Dis1RetVocher,
                @Dis2RetVocher,
                @Dis3RetVocher,
                @AddAmountRetVocher,
                @Add1RetVocher,
                @Add2RetVocher,
                @TotalAmountNutRetVocher,
                @RetVocherQty,
                @RetPrizeVocherQty,
                @VocherQty,
                @PrizevocherQty,
                @PaymentUsanceName,
                @RetCauseName,
                GETDATE()
            )";

                    using (var command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@CustCode", item.CustCode);
                        command.Parameters.AddWithValue("@SaleVocherNo", item.SaleVocherNo);
                        command.Parameters.AddWithValue("@DealerCode", item.DealerCode ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@DealerName", item.DealerName ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@SupervisorCode", item.SupervisorCode ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@SupervisorName", item.SupervisorName ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@GoodsCode", item.GoodsCode ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@GoodsName", item.GoodsName ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@TotalAmountRetVocher", item.TotalAmountRetVocher);
                        command.Parameters.AddWithValue("@DiscountRetVocher", item.DiscountRetVocher);
                        command.Parameters.AddWithValue("@Dis1RetVocher", item.Dis1RetVocher);
                        command.Parameters.AddWithValue("@Dis2RetVocher", item.Dis2RetVocher);
                        command.Parameters.AddWithValue("@Dis3RetVocher", item.Dis3RetVocher);
                        command.Parameters.AddWithValue("@AddAmountRetVocher", item.AddAmountRetVocher);
                        command.Parameters.AddWithValue("@Add1RetVocher", item.Add1RetVocher);
                        command.Parameters.AddWithValue("@Add2RetVocher", item.Add2RetVocher);
                        command.Parameters.AddWithValue("@TotalAmountNutRetVocher", item.TotalAmountNutRetVocher);
                        command.Parameters.AddWithValue("@RetVocherQty", item.RetVocherQty);
                        command.Parameters.AddWithValue("@RetPrizeVocherQty", item.RetPrizeVocherQty);
                        command.Parameters.AddWithValue("@VocherQty", item.VocherQty);
                        command.Parameters.AddWithValue("@PrizevocherQty", item.PrizevocherQty);
                        command.Parameters.AddWithValue("@PaymentUsanceName", item.PaymentUsanceName ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@RetCauseName", item.RetCauseName ?? (object)DBNull.Value);

                        await command.ExecuteNonQueryAsync();
                        recordCount++;
                    }
                }
            }

            return recordCount;
        }

        public async Task<int> SaveCustomers(List<CustomerInfo> customers)
        {
            using (var connection = new SqlConnection(_sourceConnectionString))
            {
                await connection.OpenAsync();

                // ساخت جدول موقت با SqlCommand
                using (var cmd = new SqlCommand(@"
            CREATE TABLE #TempCustomers (
                Id INT PRIMARY KEY,
                CustCode NVARCHAR(50),
                CustomerName NVARCHAR(200),
                CustActName NVARCHAR(100),
                CustLevelName NVARCHAR(100),
                CustCtgrName NVARCHAR(50),
                AreaName NVARCHAR(50),
                StateName NVARCHAR(50)
            )", connection))
                {
                    await cmd.ExecuteNonQueryAsync();
                }

                // ایجاد و پر کردن DataTable
                var table = new DataTable();

                // اضافه کردن ستون‌ها با نوع مناسب
                table.Columns.Add("Id", typeof(int));
                table.Columns.Add("CustCode", typeof(string)).MaxLength = 50;
                table.Columns.Add("CustomerName", typeof(string)).MaxLength = 200;
                table.Columns.Add("CustActName", typeof(string)).MaxLength = 100;
                table.Columns.Add("CustLevelName", typeof(string)).MaxLength = 100;
                table.Columns.Add("CustCtgrName", typeof(string)).MaxLength = 50;
                table.Columns.Add("AreaName", typeof(string)).MaxLength = 50;
                table.Columns.Add("StateName", typeof(string)).MaxLength = 50;

                // پر کردن ردیف‌ها
                foreach (var customer in customers)
                {
                    table.Rows.Add(
                        customer.Id,
                        customer.CustCode ?? (object)DBNull.Value,
                        customer.CustomerName ?? (object)DBNull.Value,
                        customer.CustActName ?? (object)DBNull.Value,
                        customer.CustLevelName ?? (object)DBNull.Value,
                        customer.CustCtgrName ?? (object)DBNull.Value,
                        customer.AreaName ?? (object)DBNull.Value,
                        customer.StateName ?? (object)DBNull.Value
                    );
                }

                // کپی داده‌ها به جدول موقت
                using (var bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = "#TempCustomers";
                    await bulkCopy.WriteToServerAsync(table);
                }

                // اجرای MERGE
                var affectedRows = await connection.ExecuteAsync(@"
            MERGE INTO dbo.Customers AS target
            USING #TempCustomers AS source
            ON target.Id = source.Id
            WHEN MATCHED THEN
                UPDATE SET 
                    target.CustCode = source.CustCode,
                    target.CustomerName = source.CustomerName,
                    target.CustActName = source.CustActName,
                    target.CustLevelName = source.CustLevelName,
                    target.CustCtgrName = source.CustCtgrName,
                    target.AreaName = source.AreaName,
                    target.StateName = source.StateName
            WHEN NOT MATCHED THEN
                INSERT (Id, CustCode, CustomerName, CustActName, 
                       CustLevelName, CustCtgrName, AreaName, StateName)
                VALUES (source.Id, source.CustCode, source.CustomerName, source.CustActName,
                        source.CustLevelName, source.CustCtgrName, source.AreaName, source.StateName);

            DROP TABLE #TempCustomers;
        ");

                return affectedRows;
            }
        }

        public async Task<int> ClearSalesData(string fromDate = null, string toDate = null)
        {
            using (var connection = new SqlConnection(_sourceConnectionString))
            {
                await connection.OpenAsync();

                string query;
                if (string.IsNullOrEmpty(fromDate) || string.IsNullOrEmpty(toDate))
                {
                    query = "DELETE FROM dbo.Sale";
                }
                else
                {
                    query = "DELETE FROM dbo.Sale WHERE SaleDate BETWEEN @FromDate AND @ToDate";
                }

                using (var command = new SqlCommand(query, connection))
                {
                    if (!string.IsNullOrEmpty(fromDate) && !string.IsNullOrEmpty(toDate))
                    {
                        command.Parameters.AddWithValue("@FromDate", fromDate);
                        command.Parameters.AddWithValue("@ToDate", toDate);
                    }

                    return await command.ExecuteNonQueryAsync();
                }
            }
        }

        public async Task<int> ClearReturnData(string fromDate = null, string toDate = null)
        {
            using (var connection = new SqlConnection(_sourceConnectionString))
            {
                await connection.OpenAsync();

                string query;
                if (string.IsNullOrEmpty(fromDate) || string.IsNullOrEmpty(toDate))
                {
                    query = "DELETE FROM dbo.SaleReturns";
                }
                else
                {
                    query = "DELETE FROM dbo.SaleReturns /*WHERE ReturnDate BETWEEN @FromDate AND @ToDate*/";
                }

                using (var command = new SqlCommand(query, connection))
                {
                    if (!string.IsNullOrEmpty(fromDate) && !string.IsNullOrEmpty(toDate))
                    {
                        command.Parameters.AddWithValue("@FromDate", fromDate);
                        command.Parameters.AddWithValue("@ToDate", toDate);
                    }

                    return await command.ExecuteNonQueryAsync();
                }
            }
        }

        public async Task<int> ClearCustomersData()
        {
            using (var connection = new SqlConnection(_sourceConnectionString))
            {
                await connection.OpenAsync();
                var query = "DELETE FROM Customers";

                using (var command = new SqlCommand(query, connection))
                {
                    return await command.ExecuteNonQueryAsync();
                }
            }
        }

        // 📊 متدهای جدید برای گزارش‌گیری
        public async Task<int> GetSalesCount()
        {
            // از دیتابیس مبدا می‌خواند
            using (var connection = new SqlConnection(_sourceConnectionString))
            {
                await connection.OpenAsync();
                var query = "SELECT COUNT(*) FROM Sale";
                using (var command = new SqlCommand(query, connection))
                {
                    return (int)await command.ExecuteScalarAsync();
                }
            }
        }

        public async Task<int> GetReturnsCount()
        {
            using (var connection = new SqlConnection(_sourceConnectionString))
            {
                await connection.OpenAsync();
                var query = "SELECT COUNT(*) FROM SaleReturns";

                using (var command = new SqlCommand(query, connection))
                {
                    return (int)await command.ExecuteScalarAsync();
                }
            }
        }

        public async Task<int> GetCustomersCount()
        {
            using (var connection = new SqlConnection(_sourceConnectionString))
            {
                await connection.OpenAsync();
                var query = "SELECT COUNT(*) FROM Customers";

                using (var command = new SqlCommand(query, connection))
                {
                    return (int)await command.ExecuteScalarAsync();
                }
            }
        }
    }
}
