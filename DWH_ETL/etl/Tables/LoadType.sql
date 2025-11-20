CREATE TABLE [etl].[LoadType] (
    [LoadTypeID]   SMALLINT      NOT NULL,
    [LoadTypeName] VARCHAR (200) NOT NULL,
    [isFullLoad]   TINYINT       NOT NULL,
    CONSTRAINT [PK_LoadType] PRIMARY KEY CLUSTERED ([LoadTypeID] ASC)
);

