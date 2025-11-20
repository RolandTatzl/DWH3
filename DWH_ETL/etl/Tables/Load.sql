CREATE TABLE [etl].[Load] (
    [LoadID]       INT            NOT NULL,
    [LoadTypeID]   SMALLINT       NOT NULL,
    [LoadStatusID] SMALLINT       NOT NULL,
    [StartDate]    DATETIME2 (2)  NOT NULL,
    [EndDate]      DATETIME2 (2)  NULL,
    [isFullLoad]   TINYINT        NOT NULL,
    [Message]      VARCHAR (1000) NULL,
    CONSTRAINT [PK_LoadProcess] PRIMARY KEY CLUSTERED ([LoadID] ASC),
    CONSTRAINT [FK_Load_LoadStatus] FOREIGN KEY ([LoadStatusID]) REFERENCES [etl].[LoadStatus] ([LoadStatusID]),
    CONSTRAINT [FK_Load_LoadType] FOREIGN KEY ([LoadTypeID]) REFERENCES [etl].[LoadType] ([LoadTypeID])
);

