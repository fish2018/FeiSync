const normalize = (expression: string) => expression.trim().replace(/\s+/g, ' ');

export const normalizeCronExpression = (expression: string) => normalize(expression);

interface CronFieldSet {
  wildcard: boolean;
  values: number[];
  valueSet: Set<number>;
}

interface ParsedCron {
  source: string;
  minutes: CronFieldSet;
  hours: CronFieldSet;
  daysOfMonth: CronFieldSet;
  months: CronFieldSet;
  daysOfWeek: CronFieldSet;
}

type AliasDict = Record<string, number>;

const MONTH_ALIASES: AliasDict = {
  jan: 1,
  feb: 2,
  mar: 3,
  apr: 4,
  may: 5,
  jun: 6,
  jul: 7,
  aug: 8,
  sep: 9,
  oct: 10,
  nov: 11,
  dec: 12
};

const WEEK_ALIASES: AliasDict = {
  sun: 0,
  mon: 1,
  tue: 2,
  wed: 3,
  thu: 4,
  fri: 5,
  sat: 6
};

const range = (start: number, end: number, step: number) => {
  const result: number[] = [];
  for (let value = start; value <= end; value += step) {
    result.push(value);
  }
  return result;
};

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));

const parseAlias = (value: string, dict?: AliasDict, allowWrap = false) => {
  const lowered = value.toLowerCase();
  if (dict && lowered in dict) {
    return dict[lowered];
  }
  const parsed = Number(value);
  if (Number.isNaN(parsed)) {
    throw new Error(`无法解析 "${value}"`);
  }
  if (allowWrap && parsed === 7) {
    return 0;
  }
  return parsed;
};

const expandToken = (
  token: string,
  min: number,
  max: number,
  aliasDict?: AliasDict,
  allowWrap = false
): { wildcard: boolean; values: number[] } => {
  let wildcard = false;
  const clean = token.trim();
  if (!clean) {
    throw new Error('Cron 字段不能为空');
  }
  if (clean === '?') {
    wildcard = true;
    return { wildcard, values: range(min, max, 1) };
  }
  const items = clean.split(',');
  const results = new Set<number>();
  for (const rawItem of items) {
    const item = rawItem.trim();
    if (!item) continue;
    const [rangePartRaw, stepPartRaw] = item.split('/');
    const step = stepPartRaw ? Number(stepPartRaw) : 1;
    if (!Number.isFinite(step) || step <= 0) {
      throw new Error(`无效的步长：${item}`);
    }
    const rangePart = rangePartRaw.trim();
    if (rangePart === '*') {
      wildcard = true;
      range(min, max, step).forEach((value) => results.add(value));
      continue;
    }
    if (rangePart.includes('-')) {
      const [startText, endText] = rangePart.split('-');
      let start = parseAlias(startText, aliasDict, allowWrap);
      let end = parseAlias(endText, aliasDict, allowWrap);
      start = clamp(start, min, max);
      end = clamp(end, min, max);
      if (end < start) {
        throw new Error(`范围必须从小到大：${rangePart}`);
      }
      for (let value = start; value <= end; value += step) {
        results.add(value);
      }
      continue;
    }
    let value = parseAlias(rangePart, aliasDict, allowWrap);
    value = clamp(value, min, max);
    results.add(value);
  }
  return {
    wildcard,
    values: [...results.values()].sort((a, b) => a - b)
  };
};

const toFieldSet = (result: { wildcard: boolean; values: number[] }): CronFieldSet => ({
  wildcard: result.wildcard,
  values: result.values,
  valueSet: new Set(result.values)
});

export const parseCronExpression = (expression: string): ParsedCron => {
  const normalized = normalize(expression);
  const parts = normalized.split(' ');
  if (parts.length !== 5) {
    throw new Error('Cron 表达式必须包含 5 个字段（分 时 日 月 周）');
  }
  const [minuteText, hourText, domText, monthText, dowText] = parts;
  return {
    source: normalized,
    minutes: toFieldSet(expandToken(minuteText, 0, 59)),
    hours: toFieldSet(expandToken(hourText, 0, 23)),
    daysOfMonth: toFieldSet(expandToken(domText, 1, 31)),
    months: toFieldSet(expandToken(monthText, 1, 12, MONTH_ALIASES)),
    daysOfWeek: toFieldSet(expandToken(dowText, 0, 6, WEEK_ALIASES, true))
  };
};

const matchesField = (field: CronFieldSet, value: number) => field.valueSet.has(value);

const matchesDay = (cron: ParsedCron, date: Date) => {
  const domWildcard = cron.daysOfMonth.wildcard;
  const dowWildcard = cron.daysOfWeek.wildcard;
  const domMatch = matchesField(cron.daysOfMonth, date.getDate());
  const dow = date.getDay();
  const dowMatch = matchesField(cron.daysOfWeek, dow);
  if (domWildcard && dowWildcard) return true;
  if (domWildcard) return dowMatch;
  if (dowWildcard) return domMatch;
  return domMatch || dowMatch;
};

const cronMatchesDate = (cron: ParsedCron, date: Date) => {
  if (!matchesField(cron.minutes, date.getMinutes())) return false;
  if (!matchesField(cron.hours, date.getHours())) return false;
  if (!matchesField(cron.months, date.getMonth() + 1)) return false;
  if (!matchesDay(cron, date)) return false;
  return true;
};

export const computeNextOccurrence = (expression: string, fromDate: Date = new Date()): Date | null => {
  const cron = parseCronExpression(expression);
  const cursor = new Date(fromDate.getTime());
  cursor.setSeconds(0, 0);
  cursor.setMinutes(cursor.getMinutes() + 1);
  const limit = 365 * 24 * 60;
  for (let index = 0; index < limit; index += 1) {
    if (cronMatchesDate(cron, cursor)) {
      return new Date(cursor.getTime());
    }
    cursor.setMinutes(cursor.getMinutes() + 1);
  }
  return null;
};

export const isCronExpressionValid = (expression: string) => {
  try {
    parseCronExpression(expression);
    return true;
  } catch {
    return false;
  }
};

export const describeCronExpression = (expression: string) => {
  const normalized = normalize(expression);
  const [minute = '*', hour = '*', dom = '*', month = '*', dow = '*'] = normalized.split(' ');
  if (normalized === '* * * * *') return '每分钟';
  if (minute.startsWith('*/') && hour === '*' && dom === '*' && month === '*' && dow === '*') {
    const step = minute.slice(2);
    return `每 ${step} 分钟`;
  }
  if (minute === '0' && hour === '*' && dom === '*' && month === '*' && dow === '*') {
    return '每小时整点';
  }
  if (minute === '0' && hour === '0' && dom === '*' && month === '*' && dow === '*') {
    return '每天 00:00';
  }
  if (dow !== '*' && dow !== '?') {
    return `每周 ${dow} 的 ${hour.padStart(2, '0')}:${minute.padStart(2, '0')}`;
  }
  if (dom !== '*' && dom !== '?') {
    return `每月 ${dom} 日 ${hour.padStart(2, '0')}:${minute.padStart(2, '0')}`;
  }
  if (hour !== '*' && minute !== '*') {
    return `每天 ${hour.padStart(2, '0')}:${minute.padStart(2, '0')}`;
  }
  return `Cron ${normalized}`;
};
