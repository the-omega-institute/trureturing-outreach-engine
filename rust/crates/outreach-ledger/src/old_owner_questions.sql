SELECT id, COALESCE(json_extract(data,'$.owner_question_opened_at'),
                (SELECT MIN(e.at) FROM task_events e WHERE e.id=tasks.id AND e.state='awaiting-user'),
                (SELECT MIN(e.at) FROM task_events e WHERE e.id=tasks.id))
                AS opened_at, json_array_length(data,'$.result.routing.questions') AS question_count
            FROM tasks
            WHERE state='awaiting-user' AND (
                NOT ? OR (kind IN ('reply','research')
                          AND json_array_length(data,'$.result.routing.questions') > 0))
            ORDER BY opened_at NULLS LAST,id
