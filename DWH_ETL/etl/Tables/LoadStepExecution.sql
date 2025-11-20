CREATE TABLE [etl].[LoadStepExecution] (
    [LoadStepExecutionID] INT            IDENTITY (1, 1) NOT NULL,
    [LoadID]              INT            NOT NULL,
    [StepID]              SMALLINT       NOT NULL,
    [StepExecutionID]     SMALLINT       NOT NULL,
    [StartDate]           DATETIME2 (2)  NOT NULL,
    [EndDate]             DATETIME2 (2)  NULL,
    [LoadStatusID]        SMALLINT       NOT NULL,
    [Message]             VARCHAR (1000) NULL,
    [Command]             VARCHAR (1000) NULL,
    PRIMARY KEY CLUSTERED ([LoadStepExecutionID] ASC),
    CONSTRAINT [FK_LoadStepExecution_Load] FOREIGN KEY ([LoadID]) REFERENCES [etl].[Load] ([LoadID]),
    CONSTRAINT [FK_LoadStepExecution_LoadStatus] FOREIGN KEY ([LoadStatusID]) REFERENCES [etl].[LoadStatus] ([LoadStatusID])
);

