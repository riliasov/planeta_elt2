-- Seed reference tables with common aliases and data
-- These can be updated later from Sheets as well

-- Сотрудники
INSERT INTO "references".employees (full_name, role, aliases)
VALUES 
    ('Администратор', 'admin', ARRAY['админ', 'admin']),
    ('Стандартный Тренер', 'trainer', ARRAY['тренер', 'coach'])
ON CONFLICT (full_name) DO NOTHING;

-- Категории расходов
INSERT INTO "references".expense_categories (name, aliases)
VALUES
    ('Аренда', ARRAY['rent']),
    ('Маркетинг', ARRAY['marketing', 'реклама']),
    ('Зарплата', ARRAY['salary', 'зп'])
ON CONFLICT (name) DO NOTHING;
