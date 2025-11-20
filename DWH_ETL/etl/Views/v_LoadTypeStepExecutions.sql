CREATE   VIEW [etl].[v_LoadTypeStepExecutions] AS
SELECT lt.LoadTypeID,
       lt.LoadTypeName,
       lts.StepID,
       lts.StepName,
       lts.isActive,
       ltse.ExecutionID,
       ltse.ExecutionString
  FROM etl.LoadType lt 
  JOIN etl.LoadTypeStep lts 
    ON lt.LoadTypeID = lts.LoadTypeID 
  JOIN etl.LoadTypeStepExecution ltse 
    ON ltse.LoadTypeID = lts.LoadTypeID 
   AND ltse.StepID = lts.StepID
