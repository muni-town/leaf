import { BytesWrapper, decode } from '@atcute/cbor';
import type { SqlRows } from '@muni-town/leaf-client';

export function stringifyEvent(event: SqlRows): string {
	const updateToJson = (v: any) => {
		if (Array.isArray(v)) {
			for (const r of v) {
				updateToJson(r);
			}
			return;
		}
		if (typeof v == 'object') {
			if (!v) {
				return v;
			} else if (v instanceof BytesWrapper) {
				v.toJSON = () => ({
					$bytes: v.$bytes,
					$bytesAsString: new TextDecoder().decode(v.buf)
				});
			} else if ('toJSON' in v) {
				return v;
			} else if ('$type' in v && v.$type == 'muni.town.sqliteValue.integer') {
				v.toJSON = () => v.value;
			} else if ('$type' in v && v.$type == 'muni.town.sqliteValue.text') {
				v.toJSON = () => v.value;
			} else if ('$type' in v && v.$type == 'muni.town.sqliteValue.blob') {
				v.toJSON = () => {
					try {
						const d = decode(v.value);
						updateToJson(d);
						return { $drisl: d };
					} catch (_e) {
						return {
							$bytes: new BytesWrapper(v.value).$bytes,
							$bytesAsString: new TextDecoder().decode(v.value)
						};
					}
				};
			} else {
				for (const key in v) {
					updateToJson(v[key]);
				}
				return;
			}
		}
	};
	updateToJson(event);
	return JSON.stringify(event, null, '  ');
}
