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

import { useEffect, useRef, useState } from 'react';
import { Bot } from 'lucide-react';
import { Button } from '~/components/ui/button';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '~/components/ui/tooltip';
import { Drawer, DrawerContent, DrawerDescription, DrawerTitle } from '~/components/ui/drawer';
import { AssistPanel } from '~/features/assist/components/AssistPanel';
import { useIsMobile } from '~/hooks/use-mobile';
import { PANEL_WIDTH_KEY, clampWidth, loadPersistedWidth } from '~/features/assist/panelWidth';

interface YarnAssistLauncherProps {
  open: boolean;
  onToggle: () => void;
}

export function YarnAssistLauncher({ open, onToggle }: YarnAssistLauncherProps) {
  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <Button
            variant="ghost"
            size="icon"
            onClick={onToggle}
            aria-label={open ? 'Close AI Assist' : 'Open AI Assist'}
            aria-expanded={open}
          >
            <Bot className="h-4 w-4" />
          </Button>
        </TooltipTrigger>
        <TooltipContent>
          <p>{open ? 'Close AI Assist' : 'Open AI Assist'}</p>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

interface YarnAssistSurfaceProps {
  open: boolean;
  onClose: () => void;
}

export function YarnAssistSurface({ open, onClose }: YarnAssistSurfaceProps) {
  const isMobile = useIsMobile();
  const [panelWidth, setPanelWidth] = useState<number>(loadPersistedWidth);
  const [viewportWidth, setViewportWidth] = useState(() =>
    typeof window !== 'undefined' ? window.innerWidth : 1280,
  );

  const isDraggingRef = useRef(false);
  const dragStartXRef = useRef(0);
  const dragStartWidthRef = useRef(panelWidth);

  // Track viewport width for responsive cap
  useEffect(() => {
    function handleResize() {
      setViewportWidth(window.innerWidth);
    }
    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  // Persist clamped panel width
  useEffect(() => {
    try {
      localStorage.setItem(PANEL_WIDTH_KEY, JSON.stringify(clampWidth(panelWidth)));
    } catch {
      // ignore storage errors
    }
  }, [panelWidth]);

  const effectiveWidth = Math.min(panelWidth, viewportWidth);

  function handleDragStart(e: React.MouseEvent) {
    e.preventDefault();
    isDraggingRef.current = true;
    dragStartXRef.current = e.clientX;
    dragStartWidthRef.current = panelWidth;

    function onMouseMove(ev: MouseEvent) {
      if (!isDraggingRef.current) return;
      const delta = dragStartXRef.current - ev.clientX;
      const newWidth = clampWidth(dragStartWidthRef.current + delta);
      setPanelWidth(newWidth);
    }

    function onMouseUp() {
      isDraggingRef.current = false;
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', onMouseUp);
    }

    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);
  }

  if (!open) return null;

  // Mobile: render inside a bottom drawer
  if (isMobile) {
    return (
      <Drawer
        open={open}
        onOpenChange={(o) => {
          if (!o) onClose();
        }}
        shouldScaleBackground={false}
      >
        <DrawerContent className="max-h-[90dvh] flex flex-col p-0">
          <DrawerTitle className="sr-only">YARN AI Assist</DrawerTitle>
          <DrawerDescription className="sr-only">
            AI assistant for YARN Capacity Scheduler configuration
          </DrawerDescription>
          <AssistPanel onClose={onClose} containerRole="region" />
        </DrawerContent>
      </Drawer>
    );
  }

  // Desktop: docked aside that participates in the horizontal layout
  return (
    <aside
      aria-label="YARN AI Assist"
      className="flex shrink-0 border-l bg-background min-h-0"
      style={{ width: effectiveWidth }}
    >
      {/* Resize handle */}
      <div
        className="w-1 cursor-col-resize hover:bg-border active:bg-primary transition-colors shrink-0"
        onMouseDown={handleDragStart}
        aria-hidden
      />
      <div className="flex-1 min-w-0 min-h-0 overflow-hidden">
        <AssistPanel onClose={onClose} containerRole="complementary" />
      </div>
    </aside>
  );
}
