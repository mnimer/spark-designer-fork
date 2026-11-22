/** @fileoverview Type definitions for pipeline palette nodes.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import type {
  InputArity,
  ReadMode,
  StreamingSinkMode,
} from '../../../../src/shared/sparkNodeCapabilities';

/**
 * Definition of a pipeline node displayed in the palette catalog.
 */
export interface NodeDefinition {
  label: string;
  type: 'SOURCE' | 'STEP' | 'SINK';
  iconName: string;
  sparkTransform: string;
  badge: string;
  description: string;
  category: string;
  supportedLanguages?: ('Java' | 'Python' | 'Go' | 'TypeScript')[];
  supportedReadModes?: ReadMode[];
  defaultReadMode?: ReadMode;
  streamingSinkMode?: StreamingSinkMode;
  errorOutput?: boolean;
  inputArity?: InputArity;
}
