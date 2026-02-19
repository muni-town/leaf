<script lang="ts">
	import { getContext } from 'svelte';
	import { decode } from '@atcute/cbor';
	import { type SqlRow, type SqlRows, type SqlValue } from '@muni-town/leaf-client';
	import { stringifyEvent } from '$lib/utils';

	const events = getContext<Array<string | SqlRows>>('events');

	let viewMode = $state<'table' | 'raw'>('table');
	let filterRule = $state('');

	const parsedFilter = $derived.by(() => {
		const idx = filterRule.indexOf('=');
		if (idx === -1) return null;
		const [column, ...path] = filterRule.slice(0, idx).trim().split('.');
		return { column, path, value: filterRule.slice(idx + 1) };
	});

	function getNestedValue(obj: unknown, path: string[]): unknown {
		let current = obj;
		for (const key of path) {
			if (current === null || typeof current !== 'object' || Array.isArray(current)) return undefined;
			current = (current as Record<string, unknown>)[key];
		}
		return current;
	}

	function primitiveMatchesFilter(v: unknown, filterValue: string): boolean {
		if (typeof v === 'string') return v === filterValue;
		if (typeof v === 'number' || typeof v === 'boolean') return String(v) === filterValue;
		return false;
	}

	function rowMatchesFilter(
		row: SqlRow,
		filter: { column: string; path: string[]; value: string }
	): boolean {
		const v = row[filter.column];
		if (!v) return false;
		if (filter.path.length === 0) {
			switch (v.$type) {
				case 'muni.town.sqliteValue.text':
					return v.value === filter.value;
				case 'muni.town.sqliteValue.integer':
				case 'muni.town.sqliteValue.real':
					return String(v.value) === filter.value;
				default:
					return false;
			}
		}
		if (v.$type !== 'muni.town.sqliteValue.blob') return false;
		try {
			const decoded = decode(v.value);
			return primitiveMatchesFilter(getNestedValue(decoded, filter.path), filter.value);
		} catch {
			return false;
		}
	}

	function applyFilter(rows: SqlRows): SqlRows {
		return parsedFilter ? rows.filter((row) => rowMatchesFilter(row, parsedFilter)) : rows;
	}

	function formatCell(v: SqlValue): string {
		switch (v.$type) {
			case 'muni.town.sqliteValue.null':
				return '';
			case 'muni.town.sqliteValue.integer':
			case 'muni.town.sqliteValue.real':
				return String(v.value);
			case 'muni.town.sqliteValue.text':
				return v.value;
			case 'muni.town.sqliteValue.blob':
				try {
					return JSON.stringify(decode(v.value), null, 2);
				} catch {
					return `<${v.value.byteLength} bytes>`;
				}
		}
	}
</script>

<div class="flex flex-col gap-3 p-2">
	<div class="flex items-center gap-3">
		<div class="join">
			<button
				type="button"
				class="btn btn-xs join-item"
				class:btn-active={viewMode === 'table'}
				onclick={() => (viewMode = 'table')}>Table</button
			>
			<button
				type="button"
				class="btn btn-xs join-item"
				class:btn-active={viewMode === 'raw'}
				onclick={() => (viewMode = 'raw')}>Raw</button
			>
		</div>
		<input
			class="input input-xs"
			placeholder="column=value"
			bind:value={filterRule}
		/>
	</div>

	{#if viewMode === 'table'}
		{#each [...events].reverse() as item}
			{#if typeof item === 'string'}
				<pre class="text-sm">{item}</pre>
			{:else}
				{@const rows = applyFilter(item)}
				{#if rows.length === 0}
					<div class="text-sm opacity-50">(no results)</div>
				{:else}
					<div class="overflow-x-auto">
						<table class="table table-xs">
							<thead>
								<tr>
									{#each Object.keys(rows[0]) as col}
										<th>{col}</th>
									{/each}
								</tr>
							</thead>
							<tbody>
								{#each rows as row}
									<tr>
										{#each Object.values(row) as cell}
											<td class="font-mono">{formatCell(cell)}</td>
										{/each}
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
			{/if}
		{/each}
	{:else}
		{#each [...events].reverse() as item}
			{#if typeof item === 'string'}
				<pre class="text-sm">{item}</pre>
			{:else}
				<pre class="text-sm">{stringifyEvent(applyFilter(item))}</pre>
			{/if}
		{/each}
	{/if}
</div>
