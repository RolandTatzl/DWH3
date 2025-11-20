CREATE TABLE [etl].[LoadTypeStep] (
    [StepID]     SMALLINT      IDENTITY (1, 1) NOT NULL,
    [LoadTypeID] SMALLINT      NOT NULL,
    [StepName]   VARCHAR (200) NOT NULL,
    [isActive]   TINYINT       NOT NULL,
    CONSTRAINT [PK_LoadTypeStep] PRIMARY KEY CLUSTERED ([LoadTypeID] ASC, [StepID] ASC),
    CONSTRAINT [FK_LoadTypeStep_LoadType] FOREIGN KEY ([LoadTypeID]) REFERENCES [etl].[LoadType] ([LoadTypeID])
);

