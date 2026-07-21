/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { describe, it, expect } from 'vitest';
import { getAssistPresentation } from './assistPresentation';
import type { YarnPageContext } from '~/features/assist/types';

function ctx(overrides: Partial<YarnPageContext> = {}): YarnPageContext {
  return {
    page_kind: null,
    selected_queue_path: null,
    selected_node_label: null,
    search_context: null,
    is_read_only: false,
    staged_change_count: 0,
    staged_change_summary: null,
    ...overrides,
  };
}

const READ_ONLY_SUFFIX =
  'You can investigate and review proposals, but staging is unavailable in read-only mode.';

describe('getAssistPresentation', () => {
  // -------------------------------------------------------------------------
  // Structural invariants
  // -------------------------------------------------------------------------

  it('always returns exactly four prompts', () => {
    const cases: YarnPageContext[] = [
      ctx({ page_kind: 'queues' }),
      ctx({ page_kind: 'queues', selected_queue_path: 'root.analytics' }),
      ctx({ page_kind: 'node-labels' }),
      ctx({ page_kind: 'node-labels', selected_node_label: 'gpu' }),
      ctx({ page_kind: 'global-settings' }),
      ctx({ page_kind: 'placement-rules' }),
      ctx({ page_kind: null }),
    ];
    for (const c of cases) {
      const p = getAssistPresentation(c);
      expect(p.prompts).toHaveLength(4);
    }
  });

  it('marks only the first prompt as recommended on every page', () => {
    const cases: YarnPageContext[] = [
      ctx({ page_kind: 'queues' }),
      ctx({ page_kind: 'queues', selected_queue_path: 'root.a' }),
      ctx({ page_kind: 'node-labels' }),
      ctx({ page_kind: 'node-labels', selected_node_label: 'ssd' }),
      ctx({ page_kind: 'global-settings' }),
      ctx({ page_kind: 'placement-rules' }),
      ctx({ page_kind: null }),
    ];
    for (const c of cases) {
      const { prompts } = getAssistPresentation(c);
      expect(prompts[0]?.recommended).toBe(true);
      expect(prompts.slice(1).every((p) => !p.recommended)).toBe(true);
    }
  });

  // -------------------------------------------------------------------------
  // Queues - no selection
  // -------------------------------------------------------------------------

  describe('queues without a selected queue', () => {
    const presentation = getAssistPresentation(ctx({ page_kind: 'queues' }));

    it('has correct hero text', () => {
      expect(presentation.hero.leading).toBe('Capacity Scheduler ready.');
      expect(presentation.hero.accent).toBe('Explore the queue hierarchy.');
    });

    it('has correct lede', () => {
      expect(presentation.hero.lede).toBe(
        'Understand capacity, pressure, and configuration across the scheduler before making changes.',
      );
    });

    it('has correct placeholder', () => {
      expect(presentation.placeholder).toBe('Ask about queues, capacity, or scheduler health...');
    });

    it('first prompt title is Explain the queue hierarchy', () => {
      expect(presentation.prompts[0]?.title).toBe('Explain the queue hierarchy');
    });

    it('first prompt sends the exact approved text', () => {
      expect(presentation.prompts[0]?.prompt).toBe(
        "Explain the current Capacity Scheduler queue hierarchy, including each queue's configured capacity, maximum capacity, current usage, and important parent-child relationships. Call out anything unusual.",
      );
    });

    it('second prompt is Find capacity pressure', () => {
      expect(presentation.prompts[1]?.title).toBe('Find capacity pressure');
    });

    it('third prompt is Review queue configuration', () => {
      expect(presentation.prompts[2]?.title).toBe('Review queue configuration');
    });

    it('fourth prompt is Propose a safe rebalance', () => {
      expect(presentation.prompts[3]?.title).toBe('Propose a safe rebalance');
    });

    it('prompts have no unreplaced placeholders', () => {
      for (const p of presentation.prompts) {
        expect(p.prompt).not.toContain('<queue path>');
        expect(p.prompt).not.toContain('<label>');
      }
    });
  });

  // -------------------------------------------------------------------------
  // Queues - with selected queue
  // -------------------------------------------------------------------------

  describe('queues with a selected queue', () => {
    const queuePath = 'root.analytics.reporting';
    const presentation = getAssistPresentation(
      ctx({ page_kind: 'queues', selected_queue_path: queuePath }),
    );

    it('substitutes queue path into leading hero text', () => {
      expect(presentation.hero.leading).toBe(`${queuePath} is in context.`);
    });

    it('has correct accent hero text', () => {
      expect(presentation.hero.accent).toBe('Inspect its capacity and pressure.');
    });

    it('substitutes queue path into placeholder', () => {
      expect(presentation.placeholder).toBe(`Ask about ${queuePath}...`);
    });

    it('substitutes queue path into the first prompt', () => {
      expect(presentation.prompts[0]?.prompt).toContain(queuePath);
      expect(presentation.prompts[0]?.prompt).not.toContain('<queue path>');
    });

    it('substitutes queue path into all prompts', () => {
      for (const p of presentation.prompts) {
        expect(p.prompt).toContain(queuePath);
        expect(p.prompt).not.toContain('<queue path>');
        expect(p.prompt).not.toContain('<label>');
      }
    });

    it('prompt titles are correct', () => {
      expect(presentation.prompts[0]?.title).toBe('Explain this queue');
      expect(presentation.prompts[1]?.title).toBe('Diagnose queue pressure');
      expect(presentation.prompts[2]?.title).toBe('Compare with siblings');
      expect(presentation.prompts[3]?.title).toBe('Propose a safe adjustment');
    });
  });

  // -------------------------------------------------------------------------
  // Queues - with partition filter (selected_node_label on queues page)
  // -------------------------------------------------------------------------

  describe('queues with a partition filter but no queue selection', () => {
    it('uses partition as selected_node_label context for scope', () => {
      const presentation = getAssistPresentation(
        ctx({ page_kind: 'queues', selected_node_label: 'gpu', selected_queue_path: null }),
      );
      // Queues page without a selected queue still shows no-queue-selection state
      // but the context hook will pass selected_node_label as the partition
      expect(presentation.prompts).toHaveLength(4);
      expect(presentation.hero.leading).toBe('Capacity Scheduler ready.');
    });
  });

  // -------------------------------------------------------------------------
  // Node labels - no selection
  // -------------------------------------------------------------------------

  describe('node labels without a selected label', () => {
    const presentation = getAssistPresentation(ctx({ page_kind: 'node-labels' }));

    it('has correct hero text', () => {
      expect(presentation.hero.leading).toBe('Node labels ready.');
      expect(presentation.hero.accent).toBe('Audit partition access and capacity.');
    });

    it('has correct lede', () => {
      expect(presentation.hero.lede).toBe(
        'Understand which labels exist, where they are assigned, and how queues can consume their resources.',
      );
    });

    it('has correct placeholder', () => {
      expect(presentation.placeholder).toBe('Ask about node labels or partitions...');
    });

    it('prompt titles are correct', () => {
      expect(presentation.prompts[0]?.title).toBe('Audit node labels');
      expect(presentation.prompts[1]?.title).toBe('Explain label allocation');
      expect(presentation.prompts[2]?.title).toBe('Find configuration gaps');
      expect(presentation.prompts[3]?.title).toBe('Propose label fixes');
    });

    it('prompts have no unreplaced placeholders', () => {
      for (const p of presentation.prompts) {
        expect(p.prompt).not.toContain('<queue path>');
        expect(p.prompt).not.toContain('<label>');
      }
    });
  });

  // -------------------------------------------------------------------------
  // Node labels - with selected label
  // -------------------------------------------------------------------------

  describe('node labels with a selected label', () => {
    const label = 'gpu';
    const presentation = getAssistPresentation(
      ctx({ page_kind: 'node-labels', selected_node_label: label }),
    );

    it('substitutes label into leading hero text', () => {
      expect(presentation.hero.leading).toBe(`${label} is in context.`);
    });

    it('has correct accent hero text', () => {
      expect(presentation.hero.accent).toBe('Inspect its queue access and resources.');
    });

    it('substitutes label into placeholder', () => {
      expect(presentation.placeholder).toBe(`Ask about the ${label} label...`);
    });

    it('substitutes label into all prompts', () => {
      for (const p of presentation.prompts) {
        expect(p.prompt).toContain(label);
        expect(p.prompt).not.toContain('<queue path>');
        expect(p.prompt).not.toContain('<label>');
      }
    });

    it('prompt titles are correct', () => {
      expect(presentation.prompts[0]?.title).toBe('Explain this label');
      expect(presentation.prompts[1]?.title).toBe('Find stranded resources');
      expect(presentation.prompts[2]?.title).toBe('Review queue access');
      expect(presentation.prompts[3]?.title).toBe('Propose a label adjustment');
    });
  });

  // -------------------------------------------------------------------------
  // Global settings
  // -------------------------------------------------------------------------

  describe('global settings', () => {
    const presentation = getAssistPresentation(ctx({ page_kind: 'global-settings' }));

    it('has correct hero text', () => {
      expect(presentation.hero.leading).toBe('Global settings in context.');
      expect(presentation.hero.accent).toBe('Review scheduler-wide behavior.');
    });

    it('has correct placeholder', () => {
      expect(presentation.placeholder).toBe('Ask about global scheduler settings...');
    });

    it('prompt titles are correct', () => {
      expect(presentation.prompts[0]?.title).toBe('Review global settings');
      expect(presentation.prompts[1]?.title).toBe('Explain scheduler behavior');
      expect(presentation.prompts[2]?.title).toBe('Spot risky defaults');
      expect(presentation.prompts[3]?.title).toBe('Propose hardening changes');
    });
  });

  // -------------------------------------------------------------------------
  // Placement rules
  // -------------------------------------------------------------------------

  describe('placement rules', () => {
    const presentation = getAssistPresentation(ctx({ page_kind: 'placement-rules' }));

    it('has correct hero text', () => {
      expect(presentation.hero.leading).toBe('Placement rules in context.');
      expect(presentation.hero.accent).toBe('Trace where workloads land.');
    });

    it('has correct placeholder', () => {
      expect(presentation.placeholder).toBe('Ask about placement rules...');
    });

    it('prompt titles are correct', () => {
      expect(presentation.prompts[0]?.title).toBe('Audit rule ordering');
      expect(presentation.prompts[1]?.title).toBe('Explain placement flow');
      expect(presentation.prompts[2]?.title).toBe('Find shadowed rules');
      expect(presentation.prompts[3]?.title).toBe('Propose safer placement');
    });
  });

  // -------------------------------------------------------------------------
  // Unknown route fallback
  // -------------------------------------------------------------------------

  describe('unknown route fallback', () => {
    const presentation = getAssistPresentation(ctx({ page_kind: null }));

    it('has correct hero text', () => {
      expect(presentation.hero.leading).toBe('YARN Assist ready.');
      expect(presentation.hero.accent).toBe('Start with scheduler context.');
    });

    it('has correct placeholder', () => {
      expect(presentation.placeholder).toBe('Ask about this YARN scheduler...');
    });

    it('prompt titles are correct', () => {
      expect(presentation.prompts[0]?.title).toBe('Summarize the scheduler');
      expect(presentation.prompts[1]?.title).toBe('Check cluster health');
      expect(presentation.prompts[2]?.title).toBe('List active applications');
      expect(presentation.prompts[3]?.title).toBe('Review scheduler configuration');
    });

    it('first prompt sends exact approved text', () => {
      expect(presentation.prompts[0]?.prompt).toBe(
        'Summarize the current Capacity Scheduler, including queue structure, resource usage, active workload, and anything that needs attention.',
      );
    });
  });

  // -------------------------------------------------------------------------
  // Read-only mode
  // -------------------------------------------------------------------------

  describe('read-only mode appends the read-only sentence', () => {
    it('appends read-only suffix to queues lede', () => {
      const p = getAssistPresentation(ctx({ page_kind: 'queues', is_read_only: true }));
      expect(p.hero.lede).toContain(READ_ONLY_SUFFIX);
    });

    it('appends read-only suffix to node labels lede', () => {
      const p = getAssistPresentation(ctx({ page_kind: 'node-labels', is_read_only: true }));
      expect(p.hero.lede).toContain(READ_ONLY_SUFFIX);
    });

    it('appends read-only suffix to global settings lede', () => {
      const p = getAssistPresentation(ctx({ page_kind: 'global-settings', is_read_only: true }));
      expect(p.hero.lede).toContain(READ_ONLY_SUFFIX);
    });

    it('appends read-only suffix to placement rules lede', () => {
      const p = getAssistPresentation(ctx({ page_kind: 'placement-rules', is_read_only: true }));
      expect(p.hero.lede).toContain(READ_ONLY_SUFFIX);
    });

    it('appends read-only suffix to unknown route lede', () => {
      const p = getAssistPresentation(ctx({ page_kind: null, is_read_only: true }));
      expect(p.hero.lede).toContain(READ_ONLY_SUFFIX);
    });

    it('does not append read-only suffix in non-read-only mode', () => {
      const p = getAssistPresentation(ctx({ page_kind: 'queues', is_read_only: false }));
      expect(p.hero.lede).not.toContain(READ_ONLY_SUFFIX);
    });

    it('preserves all four prompts in read-only mode', () => {
      const p = getAssistPresentation(
        ctx({ page_kind: 'queues', selected_queue_path: 'root.a', is_read_only: true }),
      );
      expect(p.prompts).toHaveLength(4);
    });
  });

  // -------------------------------------------------------------------------
  // Exact prompt text checks for other pages
  // -------------------------------------------------------------------------

  it('node labels first prompt sends exact approved text', () => {
    const p = getAssistPresentation(ctx({ page_kind: 'node-labels' }));
    expect(p.prompts[0]?.prompt).toBe(
      'Audit the current YARN node labels and partitions. Summarize label assignments, accessible queues, configured capacities, and any labels or resources that appear unused or unreachable.',
    );
  });

  it('global settings first prompt sends exact approved text', () => {
    const p = getAssistPresentation(ctx({ page_kind: 'global-settings' }));
    expect(p.prompts[0]?.prompt).toBe(
      'Review the current global Capacity Scheduler settings. Explain the most operationally important values and flag risky, inconsistent, deprecated, or surprising configuration.',
    );
  });

  it('placement rules first prompt sends exact approved text', () => {
    const p = getAssistPresentation(ctx({ page_kind: 'placement-rules' }));
    expect(p.prompts[0]?.prompt).toBe(
      'Audit the current Capacity Scheduler placement rules in evaluation order. Explain what each rule matches, where it places applications, and whether its fallback behavior is safe.',
    );
  });

  it('node label with selection first prompt sends exact approved text', () => {
    const p = getAssistPresentation(ctx({ page_kind: 'node-labels', selected_node_label: 'ssd' }));
    expect(p.prompts[0]?.prompt).toBe(
      'Explain the selected node label ssd: which nodes carry it, how much resource it represents, which queues can access it, and the relevant label-specific capacity settings.',
    );
  });

  // -------------------------------------------------------------------------
  // Icons per plan
  // -------------------------------------------------------------------------

  it('queues no-selection icons are correct', () => {
    const p = getAssistPresentation(ctx({ page_kind: 'queues' }));
    expect(p.prompts[0]?.icon).toBe('list-tree');
    expect(p.prompts[1]?.icon).toBe('activity');
    expect(p.prompts[2]?.icon).toBe('shield-check');
    expect(p.prompts[3]?.icon).toBe('git-compare');
  });

  it('queues with-selection icons are correct', () => {
    const p = getAssistPresentation(ctx({ page_kind: 'queues', selected_queue_path: 'root.a' }));
    expect(p.prompts[0]?.icon).toBe('book-open');
    expect(p.prompts[1]?.icon).toBe('triangle-alert');
    expect(p.prompts[2]?.icon).toBe('git-compare');
    expect(p.prompts[3]?.icon).toBe('settings');
  });
});
