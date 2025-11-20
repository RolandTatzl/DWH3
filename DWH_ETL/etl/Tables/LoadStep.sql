CREATE TABLE [etl].[LoadStep] (
    [LoadID]       INT            NOT NULL,
    [StepID]       SMALLINT       NOT NULL,
    [StartDate]    DATETIME2 (2)  NOT NULL,
    [EndDate]      DATETIME2 (2)  NULL,
    [LoadStatusID] SMALLINT       NOT NULL,
    [Message]      VARCHAR (1000) NULL,
    [logg_LoadID]  INT            NULL,
    CONSTRAINT [LoadStep_PK] PRIMARY KEY CLUSTERED ([LoadID] ASC, [StepID] ASC),
    CONSTRAINT [FK_LoadStep_Load] FOREIGN KEY ([LoadID]) REFERENCES [etl].[Load] ([LoadID]),
    CONSTRAINT [FK_LoadStep_LoadStatus] FOREIGN KEY ([LoadStatusID]) REFERENCES [etl].[LoadStatus] ([LoadStatusID])
);

