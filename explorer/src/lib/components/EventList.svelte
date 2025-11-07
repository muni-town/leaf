<script lang="ts">
	import { getContext } from 'svelte';

	const events = getContext('events') as any[];
</script>

<pre>{events
		.map((x) => x)
		.reverse()
		.join('\n')}</pre>
