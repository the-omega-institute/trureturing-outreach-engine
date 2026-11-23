SELECT
        COALESCE(SUM(kind='shortnote' AND json_extract(request,'$.inputs.pipeline.scenario')=3
                     AND COALESCE(json_extract(request,'$.inputs.note_round'),'')!='draft'),0),
        COALESCE(SUM(json_extract(request,'$.inputs.pipeline.scenario') IN (2,4,5)),0)
        FROM tasks WHERE state!='superseded'
