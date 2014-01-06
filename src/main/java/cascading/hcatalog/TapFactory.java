package cascading.hcatalog;

import cascading.scheme.Scheme;
import cascading.tap.MultiSinkTap;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

import java.util.List;

class TapFactory {

	/**
	 * Get the corresponding source {@link Tap}
	 * 
	 * @param scheme
	 * @param paths
	 * @return a single Tap or a MultiSourceTap
	 */
	public static Tap createSourceTap(Scheme scheme, List<String> paths) {
		return createTap(scheme, paths, true);
	}

    /**
     * Get the corresponding sink {@link Tap}
     *
     * @param scheme
     * @param paths
     * @return  a single Tap or a MultiSinkTap
     */
	public static Tap createSinkTap(Scheme scheme, List<String> paths) {
		return createTap(scheme, paths, false);
	}

	private static Tap createTap(Scheme scheme, List<String> paths,
                                 boolean source) {
		int size = paths.size();

		if (size == 1) {
			// Non-partitioned table
			return new Hfs(scheme, paths.get(0));
		} else {
			Tap[] taps = new Tap[size];

			for (int i = 0; i < size; i++) {
				// one tap per partition
				taps[i] = new Hfs(scheme, paths.get(i));
			}

			// Get source tap
			if (source) {
				return new MultiSourceTap(taps);
			} else {
				return new MultiSinkTap(taps);
			}
		}
	}
}
