CREATE TABLE [etl].[LoadTypeStepExecution] (
    [StepID]          SMALLINT       NOT NULL,
    [LoadTypeID]      SMALLINT       NOT NULL,
    [ExecutionID]     SMALLINT       NOT NULL,
    [ExecutionString] VARCHAR (1000) NOT NULL,
    CONSTRAINT [PK_LoadTypeStepExecution] PRIMARY KEY CLUSTERED ([LoadTypeID] ASC, [StepID] ASC, [ExecutionID] ASC),
    CONSTRAINT [FK_LoadTypeStepExecution_LoadTypeStep] FOREIGN KEY ([LoadTypeID], [StepID]) REFERENCES [etl].[LoadTypeStep] ([LoadTypeID], [StepID])
);

