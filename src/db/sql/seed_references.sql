-- Seed reference tables with common aliases and data
-- These can be updated later from Sheets as well

-- Сотрудники
INSERT INTO lookups.employees (full_name, role, aliases)
VALUES 
    ('Система', 'admin', ARRAY['System', 'скрипт']),
    ('Администратор', 'admin', ARRAY['Админ', 'Admin']);
ON CONFLICT (full_name) DO NOTHING;

-- Примеры категорий
INSERT INTO lookups.expense_categories (name, aliases)
VALUES 
    ('Аренда', ARRAY['Оплате аренды', 'Rent']),
    ('Зарплата', ARRAY['ЗП', 'Salary']),
    ('Материалы', ARRAY['Расходники', 'Materials']);
ON CONFLICT (name) DO NOTHING;
