import { BytesWrapper, decode } from '@atcute/cbor';
import type { SqlRows } from '@muni-town/leaf-client';

export function stringifyEvent(event: SqlRows): string {
	return JSON.stringify(
		event,
		(_key, value) => {
			if (value instanceof BytesWrapper) {
				return { $bytes: value.$bytes, $bytesAsString: new TextDecoder().decode(value.buf) };
			}
			if (value !== null && typeof value === 'object' && '$type' in value) {
				switch (value.$type) {
					case 'muni.town.sqliteValue.integer':
					case 'muni.town.sqliteValue.text':
						return value.value;
					case 'muni.town.sqliteValue.blob':
						try {
							return { $drisl: decode(value.value) };
						} catch {
							return {
								$bytes: new BytesWrapper(value.value).$bytes,
								$bytesAsString: new TextDecoder().decode(value.value)
							};
						}
				}
			}
			return value;
		},
		'  '
	);
}
