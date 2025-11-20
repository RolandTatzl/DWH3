CREATE TABLE [meta].[TablesINFOR] (
    [tabinID]                     SMALLINT      IDENTITY (1, 1) NOT NULL,
    [tabin_SchemaSOURCE]          NVARCHAR (50) NULL,
    [tabin_TableNameSOURCE]       NVARCHAR (50) NOT NULL,
    [tabin_Active]                BIT           NOT NULL,
    [tabin_TableLoadPRIORITY]     SMALLINT      NOT NULL,
    [tabin_serv_ClientID]         SMALLINT      NOT NULL,
    [tabin_BusinessCompanyNumber] NVARCHAR (3)  NOT NULL,
    [tabin_bcnr_BCNRange]         INT           NOT NULL,
    [tabin_TableNameTARGET]       NVARCHAR (50) NOT NULL,
    [tabin_TableCommonName]       NVARCHAR (50) NULL,
    [tabin_IncrementDays]         SMALLINT      CONSTRAINT [DF_TablesINFOR_tabin_IncrementDays] DEFAULT ((0)) NOT NULL,
    [tabin_IncrementColumn]       NVARCHAR (10) NULL,
    CONSTRAINT [PK_TablesINFOR] PRIMARY KEY CLUSTERED ([tabin_TableNameSOURCE] ASC) WITH (FILLFACTOR = 90)
);

