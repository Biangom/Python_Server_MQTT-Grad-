try:
    with conn.cursor() as cursor:
        sql = 'UPDATE users SET email = %s WHERE email = %s'
        cursor.execute(sql, ('my@test.com', 'test@test.com'))
    conn.commit()
    print(cursor.rowcount) # 1 (affected rows)
finally:
    conn.close()