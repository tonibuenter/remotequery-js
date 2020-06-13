'use strict';

const pino = require('pino');
const fs = require('fs');

// CORE -start-

const SYSTEM_CODE = 4000;


const ANONYMOUS = 'ANONYMOUS';
const MAX_RECURSION = 40;
const MAX_INCLUDES = 100;
const MAX_WHILE = 100000;
const STATEMENT_DELIMITER = ';';
const STATEMENT_ESCAPE = '\\';
let CONTEXT_COUNTER = 1;

function processLog() {
    let p = {logLines: [], addLine: addLine, time: Date.now()};
    return p;

    function addLine(message, code) {
        p.logLines.push({message: message || 'empty message', code: code || SYSTEM_CODE, time: Date().now()})
    }
}

const Dataservice = {'processSql': null};

module.exports.Dataservice = Dataservice;

const Config = {
    'getServiceEntrySql':
        'Example: select SERVICE_ID, STATEMENTS, TAGS, ROLES from T_SERVICE_TABLE where SERVICE_ID = :serviceId'
    , 'saveServiceEntry': null, 'statementsPreprocessor': null
};
module.exports.Config = Config;


Config.logger = pino({
    level: 'info',
    prettyPrint: {
        colorize: true
    }
});


const Commands = {};

module.exports.Commands = Commands;

Commands.StartBlock = {
    'if': true,
    'if-empty': true,
    'switch': true,
    'while': true,
    'foreach': true
};


Commands.EndBlock = {
    'fi': true,
    'done': true,
    'end': true
};

Commands.Registry = {};
Commands.Registry.Node = {};


async function run(request, context) {

    context = context || {contextId: CONTEXT_COUNTER++, recursion: 0};

    if (!request.userId) {
        Config.logger.info('request has no userId set. Process continues with userId: %s (%s)', ANONYMOUS, request.serviceId);
        request.userId = ANONYMOUS
    }
    request.parameters = request.parameters || {};
    context.recursion++;

    //
    // GET SERVICE
    //
    let serviceEntry = await getServiceEntry(request.serviceId);
    if (!serviceEntry) {
        return {'exception': 'No ServiceEntry found for ' + request.serviceId}
    }

    //
    // CHECK ACCESS
    //
    let hasAccess = false;
    request.roles = request.roles || [];
    if (isEmpty(serviceEntry.roles)) {
        hasAccess = true
    } else {
        for (let role of request.roles) {
            if (serviceEntry.roles.includes(role)) {
                hasAccess = true;
                break
            }
        }
    }
    if (hasAccess) {
        Config.logger.info('access to %s for %s :ok ', request.serviceId, request.userId)
    } else {
        Config.logger.warn('no access to %s for %s (service roles: %s, request roles %s )',
            request.serviceId, request.userId, serviceEntry.roles, request.roles);
        context.statusCode = '403';
        return {'exception': 'no access'}
    }

    //
    // START PROCESSING STATEMENTS
    //
    Config.logger.info('service %s found for %s ', serviceEntry.serviceId, request.userId);
    let statementNode = await prepareCommandBlock(serviceEntry, context);
    return await processCommandBlock(statementNode, request, {}, serviceEntry, context)
}

module.exports.run = run;


function buildCommandBlockTree(root, statementList, pointer) {

    while (pointer < statementList.length) {


        let statementNode = parse_statement(statementList[pointer]);
        pointer = pointer + 1;
        if (!statementNode) {
            continue
        }

        root.children = root.children || [];
        root.children.push(statementNode);

        if (Commands.EndBlock[statementNode.cmd]) {
            return pointer
        }

        if (Commands.StartBlock[statementNode.cmd]) {
            pointer = buildCommandBlockTree(statementNode, statementList, pointer)
        }
    }
    return pointer
}

async function resolveIncludes(se, context) {
    let statements = se.statements;
    context.includes = context.includes || {};
    let statementList, resolvedList;
    const _inc = 'include';

    statementList = tokenize(statements.trim(), STATEMENT_DELIMITER,
        STATEMENT_ESCAPE);
    resolvedList = [];

    for (let stmt of statementList) {
        stmt = stmt.trim();

        if (stmt.substring(0, _inc.length) === _inc) {
            let serviceId = '';
            try {
                serviceId = parse_statement(stmt).parameter;
                let se = await getServiceEntry(serviceId);
                if (!se) {
                    // noinspection ExceptionCaughtLocallyJS
                    throw 'Did not find service for include command: ' + serviceId
                }

                let counter = context.includes[se.serviceId] || 0;
                counter += 1;
                if (counter < MAX_INCLUDES) {
                    context.includes[se.serviceId] = counter;
                    let resolvedList2 = await resolveIncludes(se, context);

                    for (let j = 0; j < resolvedList2.length; ++j) {
                        let s = resolvedList2[j];
                        resolvedList.push(s)
                    }
                } else {
                    Config.logger.error('include command overflow:' + se.serviceId)
                }

            } catch (err) {
                Config.logger.error(err);
                resolvedList.push('systemMessage:include-of-error-serviceId: ' + se.serviceId);
            }
        } else {
            resolvedList.push(stmt);
        }
    }
    return resolvedList;

}

async function prepareCommandBlock(se, context) {
    context = context || {};
    let statementList = await resolveIncludes(se, context);
    let statementNode = {'cmd': 'serviceRoot', 'parameter': se.serviceId, 'statement': ''};
    buildCommandBlockTree(statementNode, statementList, 0);
    return statementNode
}

module.exports.prepareCommandBlock = prepareCommandBlock;

async function processCommandBlock(statementNode, request, currentResult,
                                   serviceEntry, context) {
    context.recursion = context.recursion || 0;
    context.recursion++;

    if (context.recursion > MAX_RECURSION) {
        let msg = 'recursion limit reached with: ' + MAX_RECURSION + '. stop processing.';
        Config.logger.error(msg);
        return {exception: msg}
    }

    let fun = Commands.Registry[statementNode.cmd];
    if (typeof fun == 'function') {
        try {
            return await fun(request, currentResult, statementNode, serviceEntry, context);
        } catch (e) {
            return {exception: e.message}
        } finally {
            context.recursion--
        }
    } else {
        Config.logger.error('unknown command: %s in statement: %s', statementNode.cmd, statementNode.statement);
        return {exception: 'no command'}
    }
}

async function getServiceEntry(serviceId) {
    let result = await Dataservice.processSql(Config.getServiceEntrySql, {'serviceId': serviceId});
    let serviceEntry = toList(result)[0];
    if (!serviceEntry) {
        return;
    }
    if (serviceEntry.roles) {
        serviceEntry.roles = serviceEntry.roles.split(/\s*,\s*/);
        for (let i = 0; i < serviceEntry.roles.length; i++) {
            serviceEntry.roles[i] = trim(serviceEntry.roles[i]);
        }
    } else {
        serviceEntry.roles = [];
    }
    if (serviceEntry.tags) {
        serviceEntry.tags = new Set(serviceEntry.tags.split(/\s*,\s*/));
    } else {
        serviceEntry.tags = new Set([]);
    }
    if (Config.statementsPreprocessor) {
        serviceEntry.statements = Config.statementsPreprocessor(serviceEntry.statements);
    }
    return serviceEntry;
}

module.exports.getServiceEntry = getServiceEntry;


//
// COMMANDs -start- ...
//

async function serviceRootCommand(request, currentResult, statementNode, serviceEntry, context) {
    for (let snChild of statementNode.children || []) {
        let r = await processCommandBlock(snChild, request, currentResult, serviceEntry, context);
        currentResult = r || currentResult;
    }
    return currentResult;
}

Commands.Registry['serviceRoot'] = serviceRootCommand;


async function sqlCommand(request, currentResult, statementNode, serviceEntry, context) {
    return await Dataservice.processSql(statementNode.parameter, request.parameters, serviceEntry, context);
}

Commands.Registry['sql'] = sqlCommand;


async function setCommand(request, currentResult, statementNode) {

    let overwrite = statementNode.cmd === 'set' || statementNode.cmd === 'copy';
    let nv = tokenize(statementNode.parameter, '=', '\\');
    let n = nv[0].trim();
    let v = nv.length > 1 ? nv[1] : '';

    v = resolve_value(v, request);
    let requestValue = request.parameters[n];

    if (overwrite || isEmpty(requestValue)) {
        request.parameters[n] = v;
    }
    return currentResult;
}

Commands.Registry['set'] = setCommand;
Commands.Registry['set-if-empty'] = setCommand;
Commands.Registry['copy'] = setCommand;
Commands.Registry['copy-if-empty'] = setCommand;


async function serviceIdCommand(request, currentResult, statementNode, serviceEntry, context) {
    let iRequest = deepClone(request);
    iRequest.serviceId = statementNode.parameter;
    return await run(iRequest, context);
}

Commands.Registry['serviceId'] = serviceIdCommand;


async function parametersCommand(request, currentResult, statementNode, serviceEntry, context) {

    let overwrite = statementNode.cmd === 'parameters';

    statementNode = parse_statement(statementNode.parameter);
    if (isEmpty(statementNode)) {
        return currentResult;
    }

    let result = await processCommandBlock(statementNode, request, currentResult,
        serviceEntry, context);
    if (!result || !result.header || result.header.length === 0) {
        return currentResult;
    }
    // empty all paramters if overwrite == true
    if (overwrite) {
        for (let head of result.header) {
            request.parameters[head] = '';
        }
    }
    let firstRow = toList(result)[0];
    if (firstRow) {
        for (let [name, value] of Object.entries(firstRow)) {
            if (!request.parameters[name] || overwrite) {
                request.parameters[name] = value;
            }
        }
    }
    return currentResult;
}

Commands.Registry['parameters'] = parametersCommand;
Commands.Registry['parameters-if-empty'] = parametersCommand;

async function ifCommand(request, currentResult, statementNode, serviceEntry, context) {

    let ifEmpty = statementNode.cmd === 'if-empty';

    let condition = resolve_value(statementNode.parameter, request);
    let isThen = !!condition;

    isThen = ifEmpty ? !isThen : isThen;

    for (let cbChild of statementNode.children || []) {
        if ("else" === cbChild.cmd) {
            isThen = !isThen;
            continue;
        }
        if (isThen) {
            let r = await processCommandBlock(cbChild, request, currentResult, serviceEntry, context);
            currentResult = r || currentResult;
        }
    }

    return currentResult;
}

Commands.Registry['if'] = ifCommand;
Commands.Registry['if-empty'] = ifCommand;


async function switchCommand(request, currentResult, statementNode, serviceEntry, context) {

    let switchValue = resolve_value(statementNode.parameter, request);
    let inSwitch = false;
    let caseFound = false;

    for (let cbChild of statementNode.children || []) {
        if ("break" === cbChild.cmd) {
            inSwitch = false;
            continue;
        }
        if ("case" === cbChild.cmd) {
            let caseParameter = resolve_value(cbChild.parameter, request);
            if (caseParameter === switchValue) {
                caseFound = true;
                inSwitch = true;
            }
        }
        if ("default" === cbChild.cmd) {
            inSwitch = !caseFound || inSwitch;
            continue;
        }

        if (inSwitch) {
            let r = await processCommandBlock(cbChild, request, currentResult, serviceEntry, context);
            currentResult = r || currentResult;
        }
    }
    return currentResult;
}

Commands.Registry['switch'] = switchCommand;


async function foreachCommand(request, currentResult, statementNode, serviceEntry, context) {

    let indexResult = processCommand(statementNode.parameter, request, currentResult, serviceEntry, context);
    if (!(indexResult && indexResult.table && indexResult.table.length === 0)) {
        return currentResult;
    }
    let indexList = toList(indexResult);
    let iRequest = deepClone(request);

    let index = 1;
    for (let indexParameters of indexList) {
        iRequest.parameters = {...iRequest.parameters, ...indexParameters, '$INDEX': '' + index};
        for (let cbChild of statementNode.children || []) {
            let r = await processCommandBlock(cbChild, request, currentResult, serviceEntry, context);
            currentResult = r || currentResult;
        }
        index += 1;
    }
    return currentResult;
}

Commands.Registry['foreach'] = foreachCommand;


async function whileCommand(request, currentResult, statementNode, serviceEntry, context) {

    let counter = 0;
    while (counter < MAX_WHILE) {
        let whileCondition = resolve_value(statementNode.parameter, request, context);
        if (!whileCondition) {
            break;
        }
        counter++;
        for (let cbChild of statementNode.children || []) {
            let r = await processCommandBlock(cbChild, request, currentResult, serviceEntry, context);
            currentResult = r == null ? currentResult : r;
        }
    }
    return currentResult;
}

Commands.Registry['while'] = whileCommand;


async function commentCommand(request, currentResult, statementNode) {
    if (!statementNode.parameter) {
        let comment = texting(statementNode.parameter, request.parameters);
        Config.logger.info(comment);
    }
    return currentResult;
}

Commands.Registry['comment'] = commentCommand;


async function nodeCommand(request, currentResult, statementNode, serviceEntry, context) {

    let fun = Commands.Registry.Node[statementNode.parameter];
    if (!fun) {
        Config.logger.error('No Commands.Registry.node entry found for ' + statementNode.parameter);
        return currentResult;
    }
    let result = await fun(request, currentResult, statementNode, serviceEntry, context);
    return result || currentResult;
}

Commands.Registry['node'] = nodeCommand;
Commands.Registry['java'] = nodeCommand;


async function noopCommand(request, currentResult) {
    return currentResult;
}

Commands.Registry['fi'] = noopCommand;
Commands.Registry['end'] = noopCommand;
Commands.Registry['done'] = noopCommand;
Commands.Registry['then'] = noopCommand;
Commands.Registry['else'] = noopCommand;
Commands.Registry['case'] = noopCommand;
Commands.Registry['default'] = noopCommand;
Commands.Registry['break'] = noopCommand;
Commands.Registry['do'] = noopCommand;

Commands.Registry['python'] = noopCommand;
Commands.Registry['class'] = noopCommand;
Commands.Registry['include'] = noopCommand;


//
//
// COMMANDs -end- ...
//
//

//
//
// UTILS -start-
//
//

function parse_statement(statement) {
    statement = statement.trim();
    if (isEmpty(statement)) {
        return;
    }
    let firstWhiteSpace = statement.length;
    for (let i = 0; i < statement.length; i++) {
        let ch = statement[i];
        if (/\s/.test(ch)) {
            firstWhiteSpace = i;
            break;
        }
    }

    let cmd = statement.substring(0, firstWhiteSpace).trim();
    let parameters = '';
    if (firstWhiteSpace !== statement.length) {
        parameters = statement.substring(firstWhiteSpace).trim();
    }

    if (isCmd(cmd)) {
        return {'cmd': cmd, 'parameter': parameters, 'statement': statement};
    }
    return {'cmd': 'sql', 'parameter': statement, 'statement': statement};
}

function isCmd(cmd) {
    return Commands.StartBlock[cmd] || Commands.EndBlock[cmd] || Commands.Registry[cmd];
}

function isEmpty(e) {
    return !e || (Array.isArray(e) && e.length === 0);
}

function trim(str) {
    if (!str) {
        return '';
    }
    return str.trim();
}

function tokenize2(s, delimiter, escape, keepEmpty, strip) {
    if (!s) {
        return [];
    }
    let tokens = [];
    let buf = '';
    let inescape = false;
    for (let c of s) {
        if (inescape) {
            buf += c;
            inescape = false;
            continue;
        }
        if (c === delimiter) {
            if (strip) {
                buf = buf.strip();
            }
            if (buf || keepEmpty) {
                tokens.append(buf);
            }
            buf = '';
            continue;
        }
        if (c === escape) {
            inescape = true;
            continue;
        }
        buf += c;
    }
    if (strip) {
        buf = buf.strip();
    }
    if (buf || keepEmpty) {
        tokens.append(buf);
    }
    return tokens;

}

function tokenize(string, del, esc) {
    if (!string) {
        return [];
    }
    // first we count the tokens
    let count = 1;
    let inescape = false;
    let pc = 0;
    let buf = '';
    for (let c of string) {
        if (c === del && !inescape) {
            count++;
            continue;
        }
        if (c === esc && !inescape) {
            inescape = true;
            continue;
        }
        inescape = false;
    }
    let tokens = [];

    // now we collect the characters and create all tokens
    let k = 0;
    for (let c of string) {
        if (c === del && !inescape) {
            tokens[k] = buf;
            buf = '';
            k++;
            pc = c;
            continue;
        }
        if (c === esc && !inescape) {
            inescape = true;
            pc = c;
            continue;
        }
        //
        // append
        //
        if (c !== del && pc === esc) {
            buf += pc;
        }
        buf += c;
        pc = c;
        inescape = false;
    }
    tokens[k] = buf;
    return tokens;
}

module.exports.tokenize = tokenize;


function deepClone(jsonObject) {
    var s = JSON.stringify(jsonObject);
    return JSON.parse(s);
}

module.exports.deepClone = deepClone;

function resolve_value(term, request) {
    term = trim(term);
    if (term.charAt(0) === ':') {
        return request.parameters[term.substring(1)] || '';
    }
    if (term.length > 1 && term.charAt(0) === '\'' && term.charAt(term.length - 1) === '\'') {
        return term.substring(1, term.length - 1);
    }
    return term;
}

module.exports.resolve_value = resolve_value;

// CORE -end-


//
// Supplementary functions
//

async function processSqlText(statements, source) {
    let gResult = {name: source || 'processSqlText-process', processLog: processLog(), counter: 0};

    let lines = statements.split('\n');
    let sqlStatement = '';
    for (let i = 0; i < lines.length; i++) {
        let origLine = lines[i];
        let line = lines[i].trim();
        // comment
        if (line.startsWith('--') || !line) {
            continue;
        }
        // sqlStatement end
        if (line.endsWith(';')) {
            sqlStatement += line.substring(0, line.length - 1) + '\n';
            try {
                let result = await Dataservice.processSql(sqlStatement, {}, 10000);
                logResult(result);
                gResult.counter++;
            } catch (e) {
                Config.logger.error(source + ':' + i + ': ' + e.message);
            } finally {

            }
            sqlStatement = '';
            continue;
        }
        sqlStatement += origLine + '\n';
    }
    Config.logger.info(source + ' : ' + gResult.counter + ' sql statements done.');
    return gResult;
}

module.exports.processSqlText = processSqlText;

async function processRqSqlText(rqSqlText, source) {
    let parameters = {};
    let statements = '';
    let gResult = {name: source || 'processRqSqlText-process', processLog: processLog(), counter: 0};
    try {

        let lines = rqSqlText.split('\n');
        let inComment = false;
        let inStatement = false;

        for (let line2 of lines) {
            let line = line2.trim();
            if (!line) {
                continue
            }
            // comment
            if (line.startsWith('--')) {
                if (!inComment) {
                    //
                    // execute collected
                    //
                    if (inStatement) {
                        let result = await saveRQService(parameters, statements, source);
                        logResult(result);
                        statements = '';
                        parameters = {};
                        inStatement = false;
                        gResult.counter++
                    }
                }
                inComment = true;
                processParameter(parameters, line.substring(2));
                continue
            }
            inComment = false;
            inStatement = true;
            statements += line2 + '\n'
        }
        if (inStatement) {
            let result = await saveRQService(parameters, statements, source);
            logResult(result);
            gResult.counter++
        }
    } catch (err) {
        Config.logger.error(err.message);
        Config.logger.error(err.stack)
    } finally {

    }
    Config.logger.info(source + ' : ' + gResult.counter + ' sq sql statements done.');
    return gResult


}

module.exports.processRqSqlText = processRqSqlText;


function logResult(result) {
    if (result.exception) {
        Config.logger.error(result.exception)
    } else {
        Config.logger.info('rowsAffected: %s', result.rowsAffected)
    }
}

async function saveRQService(parameters, statements, source) {
    parameters['source'] = source;
    parameters['statements'] = statements;
    return await run({'serviceId': Config.saveServiceEntry, 'roles': ['SYSTEM'], 'parameters': parameters});
}

module.exports.saveRQService = saveRQService;

function processParameter(parameters, line) {
    let p = line.split('=');
    if (p.length > 1) {
        // String name = Utils.camelCase(trim(p[0]))
        let name = trim(p[0]);
        parameters[name] = trim(p[1])
    }
}


function statementNodeEquals(actual, expected, assert) {
    assert.equal(actual.cmd, expected.cmd);
    actual.children = actual.children || [];
    expected.children = expected.children || [];
    assert.equal(actual.children.length, expected.children.length);
    for (let i = 0; i < actual.children.length; i++) {
        statementNodeEquals(actual.children[i], expected.children[i], assert)
    }
}

module.exports.statementNodeEquals = statementNodeEquals;


function toColumnList(data, columnName) {
    if (!Array.isArray(data)) {
        data = toList(data)
    }
    let columnList = [];
    for (let e of data) {
        if (e[columnName]) {
            columnList.push(e[columnName])
        }
    }
    return columnList
}

module.exports.toColumnList = toColumnList;


function trunk(s, n) {
    return (s && s.length > n) ? s.substr(0, n - 3) + '...' : s
}

module.exports.trunk = trunk;


async function initRepository(sqlDirectories, tags) {

    tags = tags || [''];

    for (let tag of tags) {
        for (let sqlDir of sqlDirectories) {
            if (!fs.existsSync(sqlDir) || !fs.lstatSync(sqlDir).isDirectory()) {
                Config.logger.warn('Directory does not exist: %s', sqlDir);
                continue
            }
            let sqlfileNames = fs.readdirSync(sqlDir);
            sqlfileNames = sqlfileNames.filter(filename => filename.includes(tag));
            //
            // SQL
            //
            for (let filename of sqlfileNames) {
                try {
                    if (filename.endsWith('.sql') && !filename.endsWith('.rq.sql')) {
                        Config.logger.info('Start loading as SQL file: %s', filename);
                        let text = fs.readFileSync(sqlDir + '/' + filename, 'utf8');
                        Config.logger.debug(text);
                        let result = await processSqlText(text, filename);
                        if (result.exception) {
                            Config.logger.error(result.exception)

                        } else {
                            Config.logger.debug(`updates: ${result}`)
                        }
                    }
                } catch (err) {
                    Config.logger.error(err)
                }
            }

            //
            // RQ.SQL
            //
            for (let filename of sqlfileNames) {
                try {
                    if (filename.endsWith('.rq.sql')) {
                        Config.logger.info('Start loading as RQ-SQL file: %s ', filename);
                        let text = fs.readFileSync(sqlDir + '/' + filename, 'utf8');
                        Config.logger.debug(text);
                        let result = await processRqSqlText(text, filename);
                        if (!result) {
                            Config.logger.error('Result is undefined! ')
                        } else if (result.exception) {
                            Config.logger.error(result.exception)
                        } else {
                            Config.logger.debug(`updates: ${result}`)
                        }
                    }
                } catch (err) {
                    Config.logger.error(err)
                }
            }
        }
    }
}

module.exports.initRepository = initRepository;

//
// FROM remotequery-0.9.0.js
//

function toList(serviceData) {
    var i, j, list = [], table, header, head, row, obj;
    if (serviceData.table && serviceData.header) {
        header = serviceData.header;
        table = serviceData.table;

        for (i = 0; i < table.length; i++) {
            obj = {};
            list.push(obj);
            row = table[i];
            for (j = 0; j < header.length; j++) {
                head = header[j];
                obj[head] = row[j]
            }
        }
    }
    return list
}

module.exports.toList = toList;


function texting(templateString, map) {
    if (typeof map !== 'object' || typeof templateString !== 'string') {
        return templateString
    }
    Object.keys(map).forEach(name => {
        let value = map[name];
        if (typeof value === 'string' || typeof value === 'number') {
            let r = new RegExp('\\:' + name, 'g');
            templateString = templateString.replace(r, value)
        }
    });
    return templateString
}

module.exports.texting = texting;
