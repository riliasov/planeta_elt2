// Файл: google_apps_script/change_log_triggers.js
function onEdit(e) {
    logChange(e, 'EDIT');
}

function onChange(e) {
    logChange(e, 'CHANGE');
}

function logChange(e, operationType) {
    const changeLogSheet = SpreadsheetApp.getActive()
        .getSheetByName('change_log') || createChangeLogSheet();

    const row = e.range.getRow();
    const values = e.range.getValues();
    const oldValues = e.oldValue ? [[e.oldValue]] : [['']];

    changeLogSheet.appendRow([
        new Date(),
        e.source.getActiveSheet().getName(),
        operationType,
        row,
        JSON.stringify(oldValues),
        JSON.stringify(values),
        Session.getActiveUser().getEmail(),
        'pending'
    ]);
}