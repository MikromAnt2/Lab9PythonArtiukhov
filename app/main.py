import os
import psycopg2
from datetime import date


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "db"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "university_db"),
        user=os.getenv("DB_USER", "uni_user"),
        password=os.getenv("DB_PASSWORD", "uni_pass"),
    )


def print_table(headers, rows, title=None):
    """Простий форматований вивід: заголовки + вирівнювання стовпців."""
    if title:
        print(f"\n=== {title} ===")

    rows_str = [[("" if v is None else str(v)) for v in row] for row in rows]

    col_widths = [len(h) for h in headers]
    for row in rows_str:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(cell))

    def fmt_row(row_vals):
        return " | ".join(val.ljust(col_widths[i]) for i, val in enumerate(row_vals))

    print(fmt_row(headers))
    print("-+-".join("-" * w for w in col_widths))
    for row in rows_str:
        print(fmt_row(row))


def init_db(conn):
    """Створення таблиць і заповнення даними, якщо порожньо."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS students (
                student_id   SERIAL PRIMARY KEY,
                last_name    VARCHAR(50) NOT NULL,
                first_name   VARCHAR(50) NOT NULL,
                patronymic   VARCHAR(50),
                address      TEXT,
                phone        VARCHAR(20) NOT NULL,
                course       SMALLINT NOT NULL CHECK (course BETWEEN 1 AND 4),
                faculty      VARCHAR(50) NOT NULL CHECK (
                                faculty IN ('аграрного менеджменту',
                                            'економіки',
                                            'інформаційних технологій')
                             ),
                group_name   VARCHAR(20) NOT NULL,
                is_headman   BOOLEAN NOT NULL DEFAULT FALSE,
                CONSTRAINT ck_phone_ua CHECK (phone ~ '^\\+380\\d{9}$')
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS subjects (
                subject_id         SERIAL PRIMARY KEY,
                name               VARCHAR(100) NOT NULL,
                hours_per_semester SMALLINT NOT NULL CHECK (hours_per_semester > 0),
                semesters_count    SMALLINT NOT NULL CHECK (semesters_count > 0)
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS exams (
                exam_id    SERIAL PRIMARY KEY,
                exam_date  DATE NOT NULL,
                student_id INT NOT NULL REFERENCES students(student_id) ON DELETE CASCADE,
                subject_id INT NOT NULL REFERENCES subjects(subject_id) ON DELETE CASCADE,
                grade      SMALLINT NOT NULL CHECK (grade BETWEEN 2 AND 5),
                CONSTRAINT exams_unique UNIQUE (exam_date, student_id, subject_id)
            );
        """)

        cur.execute("SELECT COUNT(*) FROM students;")
        (students_count,) = cur.fetchone()

        if students_count == 0:
            print("Заповнюю таблиці тестовими даними...")

            subjects_data = [
                ("Програмування", 64, 2),
                ("Бази даних", 48, 2),
                ("Економіка підприємства", 54, 1),
            ]
            for name, hours, sems in subjects_data:
                cur.execute(
                    "INSERT INTO subjects (name, hours_per_semester, semesters_count) "
                    "VALUES (%s, %s, %s);",
                    (name, hours, sems),
                )

            students_data = [
                ("Артюхов", "Мирослав", "Юрійович", "Київ, вул. Прикладна, 1",
                 "+380501112233", 4, "інформаційних технологій", "ІТ-41", True),
                ("Іваненко", "Олена", "Петрівна", "Київ, вул. Студентська, 10",
                 "+380501112234", 2, "економіки", "ЕК-21", True),
                ("Петренко", "Ігор", "Олександрович", "Київ, вул. Лісова, 5",
                 "+380501112235", 3, "аграрного менеджменту", "АМ-31", True),
                ("Сидоренко", "Юлія", "Андріївна", "Київ, просп. Миру, 3",
                 "+380501112236", 1, "інформаційних технологій", "ІТ-11", False),
                ("Коваль", "Дмитро", "Сергійович", "Київ, вул. Наукова, 7",
                 "+380501112237", 2, "інформаційних технологій", "ІТ-21", False),
                ("Шевченко", "Марина", "Ігорівна", "Київ, вул. Центральна, 9",
                 "+380501112238", 3, "економіки", "ЕК-31", False),
                ("Мельник", "Андрій", "Васильович", "Київ, вул. Політехнічна, 2",
                 "+380501112239", 4, "економіки", "ЕК-41", False),
                ("Гончар", "Ірина", "Іванівна", "Київ, вул. Хрещатик, 1",
                 "+380501112240", 1, "аграрного менеджменту", "АМ-11", False),
                ("Ткаченко", "Сергій", "Володимирович", "Київ, вул. Молодіжна, 12",
                 "+380501112241", 2, "аграрного менеджменту", "АМ-21", False),
                ("Романюк", "Катерина", "Степанівна", "Київ, вул. Лугова, 8",
                 "+380501112242", 3, "інформаційних технологій", "ІТ-31", False),
                ("Бондар", "Олексій", "Миколайович", "Київ, вул. Сонячна, 4",
                 "+380501112243", 1, "економіки", "ЕК-11", False),
            ]

            for s in students_data:
                cur.execute("""
                    INSERT INTO students
                    (last_name, first_name, patronymic, address, phone,
                     course, faculty, group_name, is_headman)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);
                """, s)

            exams_data = [
                (date(2025, 6, 10), 1, 1, 5),
                (date(2025, 6, 15), 1, 2, 4),
                (date(2025, 6, 20), 1, 3, 5),

                (date(2025, 6, 11), 2, 1, 4),
                (date(2025, 6, 16), 2, 2, 5),

                (date(2025, 6, 12), 3, 1, 3),
                (date(2025, 6, 17), 3, 2, 4),
                (date(2025, 6, 22), 3, 3, 4),

                (date(2025, 6, 13), 4, 1, 5),
                (date(2025, 6, 18), 4, 2, 5),

                (date(2025, 6, 14), 5, 1, 3),
                (date(2025, 6, 19), 5, 2, 3),

                (date(2025, 6, 15), 6, 2, 4),
                (date(2025, 6, 23), 6, 3, 5),

                (date(2025, 6, 16), 7, 1, 2),
                (date(2025, 6, 24), 7, 3, 3),

                (date(2025, 6, 17), 8, 1, 4),
                (date(2025, 6, 25), 8, 3, 4),

                (date(2025, 6, 18), 9, 2, 5),
                (date(2025, 6, 26), 9, 3, 4),

                (date(2025, 6, 19), 10, 1, 5),
                (date(2025, 6, 27), 10, 2, 4),

                (date(2025, 6, 20), 11, 2, 3),
            ]
            for e in exams_data:
                cur.execute("""
                    INSERT INTO exams (exam_date, student_id, subject_id, grade)
                    VALUES (%s, %s, %s, %s);
                """, e)

    conn.commit()


def show_table_structure(conn, table_name):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position;
        """, (table_name,))
        rows = cur.fetchall()
        headers = ["column_name", "data_type", "is_nullable", "column_default"]
        print_table(headers, rows, title=f"Структура таблиці {table_name}")


def show_table_data(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {table_name} ORDER BY 1;")
        rows = cur.fetchall()
        headers = [desc[0] for desc in cur.description]
        print_table(headers, rows, title=f"Дані таблиці {table_name}")


# ----------------------------Запити---------------------------------

def query_headmen(conn):
    """1) Всі студенти-старости за алфавітом."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT student_id, last_name, first_name, patronymic,
                   faculty, group_name
            FROM students
            WHERE is_headman = TRUE
            ORDER BY last_name, first_name;
        """)
        rows = cur.fetchall()
        headers = [d[0] for d in cur.description]
        print_table(headers, rows, title="Старости груп (за прізвищем)")


def query_avg_grade_per_student(conn):
    """2) Середній бал для кожного студента."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT s.student_id,
                   s.last_name,
                   s.first_name,
                   ROUND(AVG(e.grade), 2) AS avg_grade
            FROM students s
            JOIN exams e ON e.student_id = s.student_id
            GROUP BY s.student_id, s.last_name, s.first_name
            ORDER BY avg_grade DESC;
        """)
        rows = cur.fetchall()
        headers = [d[0] for d in cur.description]
        print_table(headers, rows, title="Середній бал студентів")


def query_subject_hours(conn):
    """3) Загальна кількість годин по кожному предмету."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT subject_id,
                   name,
                   hours_per_semester,
                   semesters_count,
                   (hours_per_semester * semesters_count) AS total_hours
            FROM subjects
            ORDER BY subject_id;
        """)
        rows = cur.fetchall()
        headers = [d[0] for d in cur.description]
        print_table(headers, rows, title="Загальна кількість годин по предметах")


def query_subject_performance(conn, subject_name):
    """4) Успішність студентів по обраному предмету (з параметром)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT s.last_name,
                   s.first_name,
                   sub.name AS subject,
                   e.grade
            FROM exams e
            JOIN students s ON e.student_id = s.student_id
            JOIN subjects sub ON e.subject_id = sub.subject_id
            WHERE sub.name = %s
            ORDER BY s.last_name, s.first_name;
        """, (subject_name,))
        rows = cur.fetchall()
        headers = [d[0] for d in cur.description]
        print_table(headers, rows,
                    title=f"Успішність студентів по предмету '{subject_name}'")


def query_students_per_faculty(conn):
    """5) Кількість студентів на кожному факультеті."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT faculty,
                   COUNT(*) AS students_count
            FROM students
            GROUP BY faculty
            ORDER BY faculty;
        """)
        rows = cur.fetchall()
        headers = [d[0] for d in cur.description]
        print_table(headers, rows, title="Кількість студентів по факультетах")


def query_cross_tab(conn):
    """6) Перехресний запит: оцінки кожного студента по кожному предмету.

    SQL повертає “довгий” формат, а ми в Python робимо “перехресну” таблицю:
    рядки — студенти, стовпці — предмети.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT s.student_id,
                   s.last_name,
                   s.first_name,
                   sub.name AS subject,
                   e.grade
            FROM students s
            JOIN exams e ON e.student_id = s.student_id
            JOIN subjects sub ON e.subject_id = sub.subject_id
            ORDER BY s.student_id, sub.name;
        """)
        rows = cur.fetchall()

    students = {}
    subjects = set()
    for student_id, ln, fn, subject, grade in rows:
        key = (student_id, f"{ln} {fn}")
        if key not in students:
            students[key] = {}
        students[key][subject] = grade
        subjects.add(subject)

    subjects = sorted(subjects)
    headers = ["student_id", "student_name"] + subjects
    out_rows = []

    for (sid, name), subj_grades in sorted(students.items(), key=lambda x: x[0][0]):
        row = [sid, name]
        for subj in subjects:
            row.append(subj_grades.get(subj, ""))
        out_rows.append(row)

    print_table(headers, out_rows, title="Перехресний запит: оцінки по предметах")


def main():
    conn = get_connection()
    try:
        init_db(conn)

        for table in ("students", "subjects", "exams"):
            show_table_structure(conn, table)
            show_table_data(conn, table)

        query_headmen(conn)
        query_avg_grade_per_student(conn)
        query_subject_hours(conn)
        query_subject_performance(conn, "Бази даних")
        query_students_per_faculty(conn)
        query_cross_tab(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
