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

import React from 'react';
import { FieldSwitch } from '~/components/ui/field-switch';
import { Badge } from '~/components/ui/badge';
import { Button } from '~/components/ui/button';
import { HighlightedText } from '~/components/search/HighlightedText';
import { Info } from 'lucide-react';
import { LegacyModeDocumentation } from '~/features/queue-management/components/LegacyModeDocumentation';

interface LegacyModeToggleProps {
  value: string;
  isStaged: boolean;
  onChange: (value: string) => void;
  property: {
    name: string;
    displayName: string;
    description: string;
  };
  disabled?: boolean;
  searchQuery?: string;
}

export const LegacyModeToggle: React.FC<LegacyModeToggleProps> = ({
  value,
  isStaged,
  onChange,
  property,
  disabled = false,
  searchQuery,
}) => {
  const currentEnabled = value === 'true';

  const labelNode = searchQuery ? (
    <HighlightedText text={property.displayName} highlight={searchQuery} />
  ) : (
    property.displayName
  );

  const descriptionNode = searchQuery ? (
    <HighlightedText text={property.description} highlight={searchQuery} />
  ) : (
    property.description
  );

  return (
    <FieldSwitch
      id={property.name}
      label={labelNode}
      disabled={disabled}
      labelSuffix={
        <LegacyModeDocumentation legacyModeEnabled={currentEnabled}>
          <Button
            variant="ghost"
            size="sm"
            className="h-5 w-5 p-0 hover:bg-transparent"
            type="button"
          >
            <Info className="h-3.5 w-3.5 text-muted-foreground" />
          </Button>
        </LegacyModeDocumentation>
      }
      description={descriptionNode}
      addon={
        isStaged ? (
          <Badge variant="outline" className="border-warning text-warning">
            Modified
          </Badge>
        ) : undefined
      }
      checked={currentEnabled}
      onCheckedChange={(checked) => {
        if (disabled) {
          return;
        }
        onChange(checked ? 'true' : 'false');
      }}
    />
  );
};
