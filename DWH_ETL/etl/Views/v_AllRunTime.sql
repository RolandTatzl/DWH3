CREATE   VIEW etl.v_AllRunTime AS
SELECT t.LoadTypeName
      ,l.LoadID
      ,l.StartDate
      ,l.EndDate
      -- Total runtime in HH:MM:SS
      ,FORMAT(DATEADD(SECOND, DATEDIFF(SECOND, l.StartDate, l.EndDate), 0), 'HH:mm:ss') AS total_runtime
      ,st.StepName
      ,s.StepID
      ,s.LoadStatusID
      ,s.StartDate AS StepStartTime
      ,s.EndDate AS StepEndtime
      -- Step runtime in HH:MM:SS
      ,FORMAT(DATEADD(SECOND, DATEDIFF(SECOND, s.StartDate, s.EndDate), 0), 'HH:mm:ss') AS StepRuntime
  FROM ETL.Load AS l
  JOIN ETL.LoadType AS t 
    ON l.LoadTypeID = t.LoadTypeID
  JOIN ETL.LoadStep AS s 
    ON s.LoadID = l.LoadID
  JOIN ETL.LoadTypeStep AS st
    ON st.LoadTypeID = l.LoadTypeID
   AND st.StepID = s.StepID
 WHERE l.LoadTypeID = 1
;
