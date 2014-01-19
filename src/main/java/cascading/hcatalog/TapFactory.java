/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
