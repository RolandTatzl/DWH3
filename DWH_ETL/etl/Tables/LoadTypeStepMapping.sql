CREATE TABLE [etl].[LoadTypeStepMapping] (
    [StepMappingID] INT           IDENTITY (1, 1) NOT NULL,
    [StepID]        SMALLINT      NOT NULL,
    [LoadTypeID]    SMALLINT      NOT NULL,
    [StepName]      VARCHAR (200) NOT NULL,
    [srcID]         SMALLINT      NOT NULL,
    [srcName]       VARCHAR (200) NOT NULL,
    [srcSystem]     VARCHAR (200) NOT NULL,
    PRIMARY KEY CLUSTERED ([StepMappingID] ASC),
    CONSTRAINT [UC_LoadTypeStepMapping] UNIQUE NONCLUSTERED ([LoadTypeID] ASC, [StepID] ASC)
);

