
/opt/transparent-publisher-build/shoup-subtle-20260911/artifacts/worker-integration-tests:     file format elf64-x86-64


Disassembly of section .text:

0000000000651e10 <valar_spiral_rs::poly::from_ntt>:
  651e10:	push   %rbp
  651e11:	push   %r15
  651e13:	push   %r14
  651e15:	push   %r13
  651e17:	push   %r12
  651e19:	push   %rbx
  651e1a:	sub    $0x1b8,%rsp
  651e21:	mov    0x20(%rdi),%rax
  651e25:	mov    %rax,0x10(%rsp)
  651e2a:	cmpb   $0x1,%fs:0xffffffffffffffa8
  651e33:	jne    6532d6 <valar_spiral_rs::poly::from_ntt+0x14c6>
  651e39:	mov    %fs:0x0,%rax
  651e42:	lea    -0x80(%rax),%r12
  651e49:	cmpq   $0x0,(%r12)
  651e4e:	jne    653312 <valar_spiral_rs::poly::from_ntt+0x1502>
  651e54:	movq   $0xffffffffffffffff,(%r12)
  651e5c:	mov    0x28(%rdi),%rax
  651e60:	test   %rax,%rax
  651e63:	je     6530e1 <valar_spiral_rs::poly::from_ntt+0x12d1>
  651e69:	mov    0x30(%rdi),%r8
  651e6d:	test   %r8,%r8
  651e70:	je     6530e1 <valar_spiral_rs::poly::from_ntt+0x12d1>
  651e76:	mov    %rax,0x100(%rsp)
  651e7e:	mov    0x18(%r12),%rbx
  651e83:	mov    0x20(%r12),%rcx
  651e88:	mov    0x10(%rsp),%rax
  651e8d:	add    $0x40,%rax
  651e91:	mov    %rax,0xa8(%rsp)
  651e99:	mov    0x10(%rsi),%rax
  651e9d:	mov    %rax,0x138(%rsp)
  651ea5:	mov    0x20(%rsi),%rax
  651ea9:	mov    %rax,0x140(%rsp)
  651eb1:	mov    0x30(%rsi),%rsi
  651eb5:	mov    0x10(%rdi),%rax
  651eb9:	mov    %rax,0x118(%rsp)
  651ec1:	lea    0x10(%rbx),%rax
  651ec5:	mov    %rax,0x110(%rsp)
  651ecd:	lea    0x0(,%r8,8),%rax
  651ed5:	mov    %rax,0xf8(%rsp)
  651edd:	lea    0x84084(%rip),%rax        # 6d5f68 <tokio::runtime::task::waker::WAKER_VTABLE+0x29d0>
  651ee4:	mov    %rax,0x60(%rsp)
  651ee9:	vpbroadcastq -0x5d5ad2(%rip),%ymm0        # 7c420 <GCC_except_table6770+0x41d0>
  651ef2:	xor    %eax,%eax
  651ef4:	xor    %edi,%edi
  651ef6:	mov    %r12,0x8(%rsp)
  651efb:	mov    %rbx,0xc0(%rsp)
  651f03:	mov    %rcx,0x30(%rsp)
  651f08:	mov    %r8,0x128(%rsp)
  651f10:	mov    %rsi,0x120(%rsp)
  651f18:	vmovdqu %ymm0,0x160(%rsp)
  651f21:	mov    %rsi,%rdx
  651f24:	imul   %rdi,%rdx
  651f28:	mov    %rdx,0x148(%rsp)
  651f30:	inc    %rdi
  651f33:	mov    %rax,0x108(%rsp)
  651f3b:	mov    %rax,0xf0(%rsp)
  651f43:	xor    %r9d,%r9d
  651f46:	mov    %rdi,0x130(%rsp)
  651f4e:	jmp    651f85 <valar_spiral_rs::poly::from_ntt+0x175>
  651f50:	mov    0x150(%rsp),%r9
  651f58:	inc    %r9
  651f5b:	addq   $0x8,0xf0(%rsp)
  651f64:	mov    0x128(%rsp),%r8
  651f6c:	cmp    %r8,%r9
  651f6f:	mov    0x120(%rsp),%rsi
  651f77:	mov    0x130(%rsp),%rdi
  651f7f:	je     6530ba <valar_spiral_rs::poly::from_ntt+0x12aa>
  651f85:	mov    0x140(%rsp),%rax
  651f8d:	mov    0x40(%rax),%rdx
  651f91:	imul   0x30(%rax),%rdx
  651f96:	cmp    %rcx,%rdx
  651f99:	ja     653238 <valar_spiral_rs::poly::from_ntt+0x1428>
  651f9f:	mov    0x10(%rsp),%rax
  651fa4:	mov    0x30(%rax),%rax
  651fa8:	mov    %rax,0xb0(%rsp)
  651fb0:	mov    0x148(%rsp),%rax
  651fb8:	mov    %r9,0x150(%rsp)
  651fc0:	add    %r9,%rax
  651fc3:	imul   %rdx,%rax
  651fc7:	mov    0x138(%rsp),%rcx
  651fcf:	lea    (%rcx,%rax,8),%rsi
  651fd3:	shl    $0x3,%rdx
  651fd7:	mov    %rbx,%rdi
  651fda:	vzeroupper
  651fdd:	call   *0x8585d(%rip)        # 6d7840 <memcpy@GLIBC_2.14>
  651fe3:	vpxor  %xmm11,%xmm11,%xmm11
  651fe8:	mov    0xa8(%rsp),%rax
  651ff0:	mov    (%rax),%rax
  651ff3:	mov    %rax,0x70(%rsp)
  651ff8:	test   %rax,%rax
  651ffb:	je     653040 <valar_spiral_rs::poly::from_ntt+0x1230>
  652001:	cmpq   $0x1,0x70(%rsp)
  652007:	jne    652490 <valar_spiral_rs::poly::from_ntt+0x680>
  65200d:	mov    0x10(%rsp),%rcx
  652012:	mov    0x30(%rcx),%r8
  652016:	mov    0x8(%rcx),%rax
  65201a:	mov    0x10(%rcx),%rdi
  65201e:	mov    0x38(%rcx),%rdx
  652022:	mov    %rdx,%rcx
  652025:	sub    $0x1,%rcx
  652029:	mov    $0x0,%esi
  65202e:	cmovae %rcx,%rsi
  652032:	test   %rdx,%rdx
  652035:	mov    %r8,0x18(%rsp)
  65203a:	je     652b38 <valar_spiral_rs::poly::from_ntt+0xd28>
  652040:	mov    %rdx,%r9
  652043:	mov    0x30(%rsp),%rdx
  652048:	cmp    %rdx,%r8
  65204b:	ja     6532b1 <valar_spiral_rs::poly::from_ntt+0x14a1>
  652051:	test   %rdi,%rdi
  652054:	je     653405 <valar_spiral_rs::poly::from_ntt+0x15f5>
  65205a:	mov    0x10(%rax),%rcx
  65205e:	cmp    $0x3,%rcx
  652062:	jb     653417 <valar_spiral_rs::poly::from_ntt+0x1607>
  652068:	je     6533cc <valar_spiral_rs::poly::from_ntt+0x15bc>
  65206e:	mov    0x8(%rax),%rax
  652072:	mov    0x38(%rax),%rcx
  652076:	mov    %rcx,0x70(%rsp)
  65207b:	mov    0x40(%rax),%rdi
  65207f:	mov    0x50(%rax),%rcx
  652083:	mov    %rcx,0x68(%rsp)
  652088:	mov    0x58(%rax),%rax
  65208c:	mov    %rax,0x38(%rsp)
  652091:	mov    0x10(%rsp),%rax
  652096:	mov    0xa8(%rax),%rax
  65209d:	mov    %rax,0x48(%rsp)
  6520a2:	add    %rax,%rax
  6520a5:	mov    %rax,0x28(%rsp)
  6520aa:	mov    %r9,%rcx
  6520ad:	mov    %rdi,0x40(%rsp)
  6520b2:	jmp    6520d5 <valar_spiral_rs::poly::from_ntt+0x2c5>
  6520b4:	data16 data16 cs nopw 0x0(%rax,%rax,1)
  6520c0:	mov    0xa0(%rsp),%rcx
  6520c8:	mov    %rcx,%rsi
  6520cb:	sub    $0x1,%rsi
  6520cf:	jb     652b1d <valar_spiral_rs::poly::from_ntt+0xd0d>
  6520d5:	mov    %rsi,%rax
  6520d8:	mov    0x18(%rsp),%rsi
  6520dd:	shrx   %rcx,%rsi,%r9
  6520e2:	mov    %r9,%r8
  6520e5:	add    %r9,%r8
  6520e8:	je     6531b1 <valar_spiral_rs::poly::from_ntt+0x13a1>
  6520ee:	mov    %rax,0xa0(%rsp)
  6520f6:	mov    %rsi,%rax
  6520f9:	or     %r8,%rax
  6520fc:	shr    $0x20,%rax
  652100:	je     652110 <valar_spiral_rs::poly::from_ntt+0x300>
  652102:	mov    %rsi,%rax
  652105:	xor    %edx,%edx
  652107:	div    %r8
  65210a:	jmp    652117 <valar_spiral_rs::poly::from_ntt+0x307>
  65210c:	nopl   0x0(%rax)
  652110:	mov    %esi,%eax
  652112:	xor    %edx,%edx
  652114:	div    %r8d
  652117:	lea    -0x1(%rcx),%eax
  65211a:	mov    $0x1,%ecx
  65211f:	shlx   %rax,%rcx,%r15
  652124:	sub    %rdx,%rsi
  652127:	mov    %rsi,0x58(%rsp)
  65212c:	test   %r9,%r9
  65212f:	je     652450 <valar_spiral_rs::poly::from_ntt+0x640>
  652135:	mov    %r9,%rax
  652138:	shl    $0x4,%rax
  65213c:	mov    %rax,0x78(%rsp)
  652141:	lea    0x0(,%r9,8),%rax
  652149:	mov    %rax,0xb8(%rsp)
  652151:	mov    $0x1,%eax
  652156:	mov    %rbx,0x20(%rsp)
  65215b:	xor    %edx,%edx
  65215d:	mov    %r8,0x90(%rsp)
  652165:	mov    %r9,0xc8(%rsp)
  65216d:	mov    %r15,0x80(%rsp)
  652175:	data16 cs nopw 0x0(%rax,%rax,1)
  652180:	mov    %r15,%rcx
  652183:	mov    %rdx,%r15
  652186:	add    %rcx,%r15
  652189:	cmp    %rdi,%r15
  65218c:	jae    65333b <valar_spiral_rs::poly::from_ntt+0x152b>
  652192:	cmp    0x38(%rsp),%r15
  652197:	jae    65332a <valar_spiral_rs::poly::from_ntt+0x151a>
  65219d:	mov    %rax,0x88(%rsp)
  6521a5:	mov    0x58(%rsp),%rax
  6521aa:	sub    %r8,%rax
  6521ad:	jb     65331e <valar_spiral_rs::poly::from_ntt+0x150e>
  6521b3:	mov    %rax,0x58(%rsp)
  6521b8:	mov    0x70(%rsp),%rax
  6521bd:	mov    (%rax,%r15,8),%rax
  6521c1:	mov    %rax,0xd8(%rsp)
  6521c9:	mov    0x68(%rsp),%rax
  6521ce:	mov    (%rax,%r15,8),%rax
  6521d2:	mov    %rax,0xd0(%rsp)
  6521da:	mov    0xb8(%rsp),%rax
  6521e2:	mov    0x20(%rsp),%rcx
  6521e7:	add    %rcx,%rax
  6521ea:	mov    %rax,0x50(%rsp)
  6521ef:	xor    %r12d,%r12d
  6521f2:	data16 data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  652200:	cmp    %r12,%r8
  652203:	je     653196 <valar_spiral_rs::poly::from_ntt+0x1386>
  652209:	lea    (%r9,%r12,1),%r15
  65220d:	cmp    %r8,%r15
  652210:	jae    6531a5 <valar_spiral_rs::poly::from_ntt+0x1395>
  652216:	mov    0x20(%rsp),%rax
  65221b:	mov    (%rax,%r12,8),%rcx
  65221f:	mov    %rcx,0xe8(%rsp)
  652227:	mov    0x50(%rsp),%rax
  65222c:	mov    (%rax,%r12,8),%rax
  652230:	mov    %rax,0x98(%rsp)
  652238:	mov    0x28(%rsp),%rbx
  65223d:	sub    %rax,%rbx
  652240:	add    %rcx,%rbx
  652243:	mov    %rbx,%rdx
  652246:	mov    0xd0(%rsp),%rax
  65224e:	mulx   %rax,%rax,%rax
  652253:	mulx   0xd8(%rsp),%r14,%rbp
  65225d:	mov    %rax,%rdx
  652260:	mov    0x48(%rsp),%r13
  652265:	mulx   %r13,%rcx,%rax
  65226a:	sub    %rcx,%r14
  65226d:	sbb    %rax,%rbp
  652270:	andn   %r13,%r14,%rax
  652275:	mov    %rax,%rcx
  652278:	shr    $1,%rcx
  65227b:	or     %rax,%rcx
  65227e:	mov    %rcx,%rax
  652281:	shr    $0x2,%rax
  652285:	or     %rcx,%rax
  652288:	mov    %rax,%rcx
  65228b:	shr    $0x4,%rcx
  65228f:	or     %rax,%rcx
  652292:	mov    %rcx,%rax
  652295:	shr    $0x8,%rax
  652299:	or     %rcx,%rax
  65229c:	mov    %rax,%rcx
  65229f:	shr    $0x10,%rcx
  6522a3:	or     %rax,%rcx
  6522a6:	mov    %rcx,%rax
  6522a9:	shr    $0x20,%rax
  6522ad:	or     %r13,%rcx
  6522b0:	or     %rax,%rcx
  6522b3:	andn   %r14,%rcx,%rax
  6522b8:	mov    %rbp,%rcx
  6522bb:	shld   $0x3f,%rax,%rcx
  6522c0:	mov    %rbp,%rdx
  6522c3:	shr    $1,%rdx
  6522c6:	or     %rax,%rcx
  6522c9:	or     %rbp,%rdx
  6522cc:	mov    %rdx,%rax
  6522cf:	shr    $0x2,%rax
  6522d3:	or     %rdx,%rax
  6522d6:	shld   $0x3e,%rcx,%rdx
  6522db:	or     %rcx,%rdx
  6522de:	mov    %rax,%rcx
  6522e1:	shr    $0x4,%rcx
  6522e5:	or     %rax,%rcx
  6522e8:	shld   $0x3c,%rdx,%rax
  6522ed:	or     %rdx,%rax
  6522f0:	mov    %rcx,%rdx
  6522f3:	shr    $0x8,%rdx
  6522f7:	or     %rcx,%rdx
  6522fa:	shld   $0x38,%rax,%rcx
  6522ff:	or     %rax,%rcx
  652302:	mov    %rdx,%rax
  652305:	shr    $0x10,%rax
  652309:	or     %rdx,%rax
  65230c:	shld   $0x30,%rcx,%rdx
  652311:	or     %rcx,%rdx
  652314:	mov    %rdx,%rcx
  652317:	shr    $0x20,%rcx
  65231b:	or     %edx,%ecx
  65231d:	mov    %rax,%rdx
  652320:	shr    $0x20,%rdx
  652324:	or     %eax,%edx
  652326:	or     %ecx,%edx
  652328:	mov    %r14,%r15
  65232b:	sub    %r13,%r15
  65232e:	and    $0x1,%dl
  652331:	movzbl %dl,%edi
  652334:	vzeroupper
  652337:	call   64f3b0 <subtle::black_box>
  65233c:	not    %al
  65233e:	and    $0x1,%al
  652340:	movzbl %al,%edi
  652343:	call   64f3b0 <subtle::black_box>
  652348:	mov    %al,0xe0(%rsp)
  65234f:	mov    %r14,%rax
  652352:	xor    %r13,%rax
  652355:	xor    %edi,%edi
  652357:	or     %rbp,%rax
  65235a:	sete   %dil
  65235e:	call   64f3b0 <subtle::black_box>
  652363:	not    %al
  652365:	and    $0x1,%al
  652367:	movzbl %al,%edi
  65236a:	call   64f3b0 <subtle::black_box>
  65236f:	and    0xe0(%rsp),%al
  652376:	movzbl %al,%edi
  652379:	call   64f3b0 <subtle::black_box>
  65237e:	mov    0xc8(%rsp),%r9
  652386:	mov    0x90(%rsp),%r8
  65238e:	movzbl %al,%eax
  652391:	mov    %rax,%rcx
  652394:	neg    %rcx
  652397:	dec    %rax
  65239a:	and    %r15,%rax
  65239d:	and    %r14,%rcx
  6523a0:	or     %rax,%rcx
  6523a3:	mov    0x98(%rsp),%rsi
  6523ab:	mov    0xe8(%rsp),%rax
  6523b3:	add    %rax,%rsi
  6523b6:	add    %rax,%rax
  6523b9:	cmp    %rbx,%rax
  6523bc:	mov    0x28(%rsp),%rax
  6523c1:	mov    $0x0,%edx
  6523c6:	cmovb  %rdx,%rax
  6523ca:	sub    %rax,%rsi
  6523cd:	test   $0x1,%bl
  6523d0:	mov    $0x0,%eax
  6523d5:	cmovne %r13,%rax
  6523d9:	add    %rsi,%rax
  6523dc:	shr    $1,%rax
  6523df:	mov    0x20(%rsp),%rdx
  6523e4:	mov    %rax,(%rdx,%r12,8)
  6523e8:	mov    0x50(%rsp),%rax
  6523ed:	mov    %rcx,(%rax,%r12,8)
  6523f1:	inc    %r12
  6523f4:	cmp    %r12,%r9
  6523f7:	jne    652200 <valar_spiral_rs::poly::from_ntt+0x3f0>
  6523fd:	mov    0x80(%rsp),%r15
  652405:	mov    0x88(%rsp),%rdx
  65240d:	cmp    %r15,%rdx
  652410:	mov    %rdx,%rax
  652413:	adc    $0x0,%rax
  652417:	mov    0x20(%rsp),%rcx
  65241c:	add    0x78(%rsp),%rcx
  652421:	mov    %rcx,0x20(%rsp)
  652426:	cmp    %r15,%rdx
  652429:	mov    0x8(%rsp),%r12
  65242e:	mov    0xc0(%rsp),%rbx
  652436:	mov    0x40(%rsp),%rdi
  65243b:	jb     652180 <valar_spiral_rs::poly::from_ntt+0x370>
  652441:	jmp    6520c0 <valar_spiral_rs::poly::from_ntt+0x2b0>
  652446:	cs nopw 0x0(%rax,%rax,1)
  652450:	xor    %eax,%eax
  652452:	data16 data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  652460:	lea    (%r15,%rax,1),%rcx
  652464:	cmp    %rdi,%rcx
  652467:	jae    653373 <valar_spiral_rs::poly::from_ntt+0x1563>
  65246d:	cmp    0x38(%rsp),%rcx
  652472:	jae    65335b <valar_spiral_rs::poly::from_ntt+0x154b>
  652478:	sub    %r8,0x58(%rsp)
  65247d:	jb     65331e <valar_spiral_rs::poly::from_ntt+0x150e>
  652483:	inc    %rax
  652486:	cmp    %r15,%rax
  652489:	jb     652460 <valar_spiral_rs::poly::from_ntt+0x650>
  65248b:	jmp    6520c0 <valar_spiral_rs::poly::from_ntt+0x2b0>
  652490:	mov    0x10(%rsp),%rcx
  652495:	mov    0x30(%rcx),%rdi
  652499:	mov    0x8(%rcx),%rax
  65249d:	mov    %rax,0x80(%rsp)
  6524a5:	mov    0x38(%rcx),%rdx
  6524a9:	mov    %rdx,0x78(%rsp)
  6524ae:	sub    $0x1,%rdx
  6524b2:	mov    $0x0,%eax
  6524b7:	cmovb  %rax,%rdx
  6524bb:	mov    %rdx,0x18(%rsp)
  6524c0:	mov    %rdi,%rdx
  6524c3:	shr    $0x2,%rdx
  6524c7:	mov    %edi,%eax
  6524c9:	and    $0x3,%eax
  6524cc:	cmp    $0x1,%rax
  6524d0:	sbb    $0xffffffffffffffff,%rdx
  6524d4:	mov    %rdx,0x158(%rsp)
  6524dc:	mov    0x10(%rcx),%rax
  6524e0:	mov    %rax,0x68(%rsp)
  6524e5:	lea    0x0(,%rdi,8),%rax
  6524ed:	mov    %rax,0xa0(%rsp)
  6524f5:	mov    $0x1,%eax
  6524fa:	mov    0x110(%rsp),%rcx
  652502:	mov    %rcx,0x38(%rsp)
  652507:	mov    %rbx,%r8
  65250a:	xor    %esi,%esi
  65250c:	mov    %rdi,0x58(%rsp)
  652511:	jmp    652550 <valar_spiral_rs::poly::from_ntt+0x740>
  652513:	data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  652520:	mov    0x70(%rsp),%rcx
  652525:	cmp    %rcx,%rsi
  652528:	mov    %rsi,%rax
  65252b:	adc    $0x0,%rax
  65252f:	mov    0xa0(%rsp),%rdx
  652537:	add    %rdx,%r8
  65253a:	add    %rdx,0x38(%rsp)
  65253f:	cmp    %rcx,%rsi
  652542:	mov    0xc0(%rsp),%rbx
  65254a:	jae    653040 <valar_spiral_rs::poly::from_ntt+0x1230>
  652550:	mov    %rax,%r9
  652553:	mov    %rdi,%rax
  652556:	imul   %rsi,%rax
  65255a:	mov    %rax,%rcx
  65255d:	add    %rdi,%rcx
  652560:	jb     6531bd <valar_spiral_rs::poly::from_ntt+0x13ad>
  652566:	mov    0x30(%rsp),%rdx
  65256b:	cmp    %rdx,%rcx
  65256e:	ja     6531d0 <valar_spiral_rs::poly::from_ntt+0x13c0>
  652574:	mov    %rsi,%r15
  652577:	cmp    0x68(%rsp),%rsi
  65257c:	jae    6533e3 <valar_spiral_rs::poly::from_ntt+0x15d3>
  652582:	lea    (%r15,%r15,2),%rax
  652586:	mov    0x80(%rsp),%rcx
  65258e:	mov    0x10(%rcx,%rax,8),%rsi
  652593:	cmp    $0x3,%rsi
  652597:	jb     65341a <valar_spiral_rs::poly::from_ntt+0x160a>
  65259d:	mov    %r8,0x88(%rsp)
  6525a5:	je     6533cc <valar_spiral_rs::poly::from_ntt+0x15bc>
  6525ab:	mov    %r9,0xb8(%rsp)
  6525b3:	cmp    $0x4,%r15
  6525b7:	jae    6533f4 <valar_spiral_rs::poly::from_ntt+0x15e4>
  6525bd:	mov    0x10(%rsp),%rcx
  6525c2:	mov    0xa8(%rcx,%r15,8),%rcx
  6525ca:	lea    (%rcx,%rcx,1),%rdx
  6525ce:	mov    %rdx,0x50(%rsp)
  6525d3:	vmovq  %rdx,%xmm0
  6525d8:	mov    %rcx,0x20(%rsp)
  6525dd:	vmovq  %rcx,%xmm1
  6525e2:	cmpq   $0x0,0x78(%rsp)
  6525e8:	je     652ab0 <valar_spiral_rs::poly::from_ntt+0xca0>
  6525ee:	mov    0x80(%rsp),%rcx
  6525f6:	lea    (%rcx,%rax,8),%rax
  6525fa:	mov    0x8(%rax),%rax
  6525fe:	mov    0x38(%rax),%rcx
  652602:	mov    %rcx,0x28(%rsp)
  652607:	mov    0x40(%rax),%r10
  65260b:	mov    0x50(%rax),%rcx
  65260f:	mov    %rcx,0x98(%rsp)
  652617:	mov    0x58(%rax),%rax
  65261b:	mov    %rax,0x40(%rsp)
  652620:	vpbroadcastq %xmm1,%ymm2
  652625:	vpbroadcastq %xmm0,%ymm3
  65262a:	mov    0x18(%rsp),%rax
  65262f:	mov    0x78(%rsp),%rcx
  652634:	mov    %r10,0xd0(%rsp)
  65263c:	jmp    65265f <valar_spiral_rs::poly::from_ntt+0x84f>
  65263e:	xchg   %ax,%ax
  652640:	mov    0xc8(%rsp),%rcx
  652648:	mov    %rcx,%rax
  65264b:	sub    $0x1,%rax
  65264f:	mov    0x8(%rsp),%r12
  652654:	mov    0x58(%rsp),%rdi
  652659:	jb     652ab0 <valar_spiral_rs::poly::from_ntt+0xca0>
  65265f:	shrx   %rcx,%rdi,%rsi
  652664:	mov    %rsi,%rbp
  652667:	add    %rsi,%rbp
  65266a:	je     6530fc <valar_spiral_rs::poly::from_ntt+0x12ec>
  652670:	mov    %rax,%r8
  652673:	mov    %rdi,%rax
  652676:	or     %rbp,%rax
  652679:	shr    $0x20,%rax
  65267d:	je     652690 <valar_spiral_rs::poly::from_ntt+0x880>
  65267f:	mov    %rdi,%rax
  652682:	xor    %edx,%edx
  652684:	div    %rbp
  652687:	jmp    652696 <valar_spiral_rs::poly::from_ntt+0x886>
  652689:	nopl   0x0(%rax)
  652690:	mov    %edi,%eax
  652692:	xor    %edx,%edx
  652694:	div    %ebp
  652696:	lea    -0x1(%rcx),%eax
  652699:	mov    $0x1,%ecx
  65269e:	shlx   %rax,%rcx,%rax
  6526a3:	mov    %rdi,%rcx
  6526a6:	sub    %rdx,%rcx
  6526a9:	mov    %rsi,%r12
  6526ac:	shr    $0x2,%r12
  6526b0:	mov    %esi,%edx
  6526b2:	and    $0x3,%edx
  6526b5:	cmp    $0x1,%rdx
  6526b9:	sbb    $0xffffffffffffffff,%r12
  6526bd:	cmp    $0x4,%rsi
  6526c1:	mov    %r8,0xc8(%rsp)
  6526c9:	jae    652910 <valar_spiral_rs::poly::from_ntt+0xb00>
  6526cf:	test   %rsi,%rsi
  6526d2:	je     652a5f <valar_spiral_rs::poly::from_ntt+0xc4f>
  6526d8:	lea    0x1(%rsi),%rdx
  6526dc:	mov    %rdx,0x48(%rsp)
  6526e1:	lea    0x2(%rsi),%rdx
  6526e5:	mov    %rdx,0x90(%rsp)
  6526ed:	mov    0x98(%rsp),%rdx
  6526f5:	lea    (%rdx,%rax,8),%rdx
  6526f9:	mov    %rdx,0xe8(%rsp)
  652701:	mov    0x28(%rsp),%rdx
  652706:	lea    (%rdx,%rax,8),%rdx
  65270a:	mov    %rdx,0xe0(%rsp)
  652712:	mov    %rsi,%rdx
  652715:	shl    $0x4,%rdx
  652719:	mov    %rdx,0xd8(%rsp)
  652721:	mov    $0x1,%edi
  652726:	mov    0x38(%rsp),%r9
  65272b:	xor    %edx,%edx
  65272d:	jmp    65274d <valar_spiral_rs::poly::from_ntt+0x93d>
  65272f:	nop
  652730:	lea    0x1(%rdx),%rdi
  652734:	add    0xd8(%rsp),%r9
  65273c:	cmp    %rax,%rdx
  65273f:	mov    0xd0(%rsp),%r10
  652747:	jae    652640 <valar_spiral_rs::poly::from_ntt+0x830>
  65274d:	lea    (%rax,%rdi,1),%r15
  652751:	dec    %r15
  652754:	cmp    %r10,%r15
  652757:	mov    0x8(%rsp),%r12
  65275c:	jae    653275 <valar_spiral_rs::poly::from_ntt+0x1465>
  652762:	mov    %rdx,%r8
  652765:	mov    %rdi,%rdx
  652768:	mov    0x40(%rsp),%rdi
  65276d:	cmp    %rdi,%r15
  652770:	jae    653256 <valar_spiral_rs::poly::from_ntt+0x1446>
  652776:	sub    %rbp,%rcx
  652779:	jb     6531f1 <valar_spiral_rs::poly::from_ntt+0x13e1>
  65277f:	mov    0xe0(%rsp),%rdi
  652787:	mov    -0x8(%rdi,%rdx,8),%rdi
  65278c:	mov    0xe8(%rsp),%r8
  652794:	mov    -0x8(%r8,%rdx,8),%r8
  652799:	mov    -0x10(%r9),%r15
  65279d:	mov    -0x10(%r9,%rsi,8),%r12
  6527a2:	mov    0x50(%rsp),%r14
  6527a7:	mov    %r14,%r10
  6527aa:	sub    %r12,%r10
  6527ad:	add    %r15,%r10
  6527b0:	mov    %r10,%rbx
  6527b3:	imul   %r8,%rbx
  6527b7:	shr    $0x20,%rbx
  6527bb:	mov    %r10,%r13
  6527be:	imul   %rdi,%r13
  6527c2:	mov    0x20(%rsp),%r11
  6527c7:	imul   %r11,%rbx
  6527cb:	sub    %rbx,%r13
  6527ce:	add    %r15,%r12
  6527d1:	add    %r15,%r15
  6527d4:	cmp    %r10,%r15
  6527d7:	mov    $0x0,%r15d
  6527dd:	cmovb  %r15,%r14
  6527e1:	sub    %r14,%r12
  6527e4:	test   $0x1,%r10b
  6527e8:	mov    $0x0,%r10d
  6527ee:	cmovne %r11,%r10
  6527f2:	add    %r12,%r10
  6527f5:	shr    $1,%r10
  6527f8:	mov    %r10,-0x10(%r9)
  6527fc:	mov    %r13,-0x10(%r9,%rsi,8)
  652801:	cmp    $0x1,%rsi
  652805:	je     652730 <valar_spiral_rs::poly::from_ntt+0x920>
  65280b:	cmp    %rbp,0x48(%rsp)
  652810:	jae    65329d <valar_spiral_rs::poly::from_ntt+0x148d>
  652816:	mov    -0x8(%r9),%r10
  65281a:	mov    -0x8(%r9,%rsi,8),%rbx
  65281f:	mov    0x50(%rsp),%r14
  652824:	mov    %r14,%r15
  652827:	sub    %rbx,%r15
  65282a:	add    %r10,%r15
  65282d:	mov    %r15,%r12
  652830:	imul   %r8,%r12
  652834:	shr    $0x20,%r12
  652838:	mov    %r15,%r13
  65283b:	imul   %rdi,%r13
  65283f:	mov    0x20(%rsp),%r11
  652844:	imul   %r11,%r12
  652848:	sub    %r12,%r13
  65284b:	add    %r10,%rbx
  65284e:	add    %r10,%r10
  652851:	cmp    %r15,%r10
  652854:	mov    %r14,%r10
  652857:	mov    $0x0,%r12d
  65285d:	cmovb  %r12,%r10
  652861:	sub    %r10,%rbx
  652864:	test   $0x1,%r15b
  652868:	mov    $0x0,%r10d
  65286e:	cmovne %r11,%r10
  652872:	add    %rbx,%r10
  652875:	shr    $1,%r10
  652878:	mov    %r10,-0x8(%r9)
  65287c:	mov    %r13,-0x8(%r9,%rsi,8)
  652881:	cmp    $0x2,%rsi
  652885:	je     652730 <valar_spiral_rs::poly::from_ntt+0x920>
  65288b:	cmp    %rbp,0x90(%rsp)
  652893:	mov    0x8(%rsp),%r12
  652898:	jae    6532bf <valar_spiral_rs::poly::from_ntt+0x14af>
  65289e:	mov    (%r9),%r10
  6528a1:	mov    (%r9,%rsi,8),%rbx
  6528a5:	mov    0x50(%rsp),%r14
  6528aa:	mov    %r14,%r15
  6528ad:	sub    %rbx,%r15
  6528b0:	add    %r10,%r15
  6528b3:	imul   %r15,%r8
  6528b7:	shr    $0x20,%r8
  6528bb:	imul   %r15,%rdi
  6528bf:	mov    0x20(%rsp),%r11
  6528c4:	imul   %r11,%r8
  6528c8:	sub    %r8,%rdi
  6528cb:	add    %r10,%rbx
  6528ce:	add    %r10,%r10
  6528d1:	cmp    %r15,%r10
  6528d4:	mov    %r14,%r8
  6528d7:	mov    $0x0,%r10d
  6528dd:	cmovb  %r10,%r8
  6528e1:	sub    %r8,%rbx
  6528e4:	test   $0x1,%r15b
  6528e8:	mov    $0x0,%r8d
  6528ee:	cmovne %r11,%r8
  6528f2:	add    %rbx,%r8
  6528f5:	shr    $1,%r8
  6528f8:	mov    %r8,(%r9)
  6528fb:	mov    %rdi,(%r9,%rsi,8)
  6528ff:	jmp    652730 <valar_spiral_rs::poly::from_ntt+0x920>
  652904:	data16 data16 cs nopw 0x0(%rax,%rax,1)
  652910:	mov    %rsi,%r8
  652913:	shl    $0x4,%r8
  652917:	lea    0x0(,%rsi,8),%r11
  65291f:	mov    $0x1,%edx
  652924:	mov    0x88(%rsp),%r14
  65292c:	xor    %r9d,%r9d
  65292f:	nop
  652930:	mov    %r9,%r15
  652933:	add    %rax,%r15
  652936:	cmp    %r10,%r15
  652939:	jae    653213 <valar_spiral_rs::poly::from_ntt+0x1403>
  65293f:	cmp    0x40(%rsp),%r15
  652944:	jae    653222 <valar_spiral_rs::poly::from_ntt+0x1412>
  65294a:	sub    %rbp,%rcx
  65294d:	jb     6531fa <valar_spiral_rs::poly::from_ntt+0x13ea>
  652953:	mov    %rdx,%r9
  652956:	mov    0x98(%rsp),%rdx
  65295e:	vpmovzxdq (%rdx,%r15,8),%xmm4
  652964:	vpbroadcastq %xmm4,%ymm4
  652969:	mov    0x28(%rsp),%rdx
  65296e:	vmovq  (%rdx,%r15,8),%xmm5
  652974:	vpmovzxdq %xmm5,%xmm5
  652979:	vpbroadcastq %xmm5,%ymm5
  65297e:	lea    (%r14,%r11,1),%r13
  652982:	xor    %edi,%edi
  652984:	mov    %r12,%rdx
  652987:	nopw   0x0(%rax,%rax,1)
  652990:	cmp    %rbp,%rdi
  652993:	jae    65313b <valar_spiral_rs::poly::from_ntt+0x132b>
  652999:	lea    (%rsi,%rdi,1),%r15
  65299d:	cmp    %rbp,%r15
  6529a0:	jae    65312f <valar_spiral_rs::poly::from_ntt+0x131f>
  6529a6:	dec    %rdx
  6529a9:	vmovdqa (%r14,%rdi,8),%ymm6
  6529af:	vmovdqa 0x0(%r13,%rdi,8),%ymm7
  6529b6:	vpsubq %ymm7,%ymm3,%ymm8
  6529ba:	vpaddq %ymm6,%ymm8,%ymm8
  6529be:	vpaddq %ymm6,%ymm6,%ymm9
  6529c2:	vpcmpgtq %ymm8,%ymm9,%ymm9
  6529c7:	vpand  %ymm3,%ymm9,%ymm9
  6529cb:	vpaddq %ymm6,%ymm7,%ymm6
  6529cf:	vpsubq %ymm9,%ymm6,%ymm6
  6529d4:	vpmuludq %ymm4,%ymm8,%ymm7
  6529d8:	vpsrlq $0x20,%ymm4,%ymm9
  6529dd:	vpmuludq %ymm9,%ymm8,%ymm9
  6529e2:	vpsllq $0x20,%ymm9,%ymm9
  6529e8:	vpaddq %ymm7,%ymm9,%ymm7
  6529ec:	vpsrlq $0x20,%ymm7,%ymm7
  6529f1:	vpsllq $0x3f,%ymm8,%ymm9
  6529f7:	vpcmpgtq %ymm9,%ymm11,%ymm9
  6529fc:	vpand  %ymm2,%ymm9,%ymm9
  652a00:	vpaddq %ymm6,%ymm9,%ymm6
  652a04:	vpsrlq $0x1,%ymm6,%ymm6
  652a09:	vpmuludq %ymm5,%ymm8,%ymm9
  652a0d:	vpsrlq $0x20,%ymm5,%ymm10
  652a12:	vpmuludq %ymm10,%ymm8,%ymm8
  652a17:	vpsllq $0x20,%ymm8,%ymm8
  652a1d:	vpaddq %ymm8,%ymm9,%ymm8
  652a22:	vpmuludq %ymm2,%ymm7,%ymm7
  652a26:	vpsubq %ymm7,%ymm8,%ymm7
  652a2a:	vmovdqa %ymm6,(%r14,%rdi,8)
  652a30:	vmovdqa %ymm7,0x0(%r13,%rdi,8)
  652a37:	add    $0x4,%rdi
  652a3b:	test   %rdx,%rdx
  652a3e:	jne    652990 <valar_spiral_rs::poly::from_ntt+0xb80>
  652a44:	cmp    %rax,%r9
  652a47:	mov    %r9,%rdx
  652a4a:	adc    $0x0,%rdx
  652a4e:	add    %r8,%r14
  652a51:	cmp    %rax,%r9
  652a54:	jb     652930 <valar_spiral_rs::poly::from_ntt+0xb20>
  652a5a:	jmp    652640 <valar_spiral_rs::poly::from_ntt+0x830>
  652a5f:	xor    %edx,%edx
  652a61:	data16 data16 data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  652a70:	lea    (%rax,%rdx,1),%rsi
  652a74:	cmp    %r10,%rsi
  652a77:	mov    0x8(%rsp),%r12
  652a7c:	jae    65326b <valar_spiral_rs::poly::from_ntt+0x145b>
  652a82:	mov    0x40(%rsp),%rdi
  652a87:	cmp    %rdi,%rsi
  652a8a:	jae    653284 <valar_spiral_rs::poly::from_ntt+0x1474>
  652a90:	sub    %rbp,%rcx
  652a93:	jb     6531f1 <valar_spiral_rs::poly::from_ntt+0x13e1>
  652a99:	inc    %rdx
  652a9c:	cmp    %rax,%rdx
  652a9f:	jb     652a70 <valar_spiral_rs::poly::from_ntt+0xc60>
  652aa1:	jmp    652640 <valar_spiral_rs::poly::from_ntt+0x830>
  652aa6:	cs nopw 0x0(%rax,%rax,1)
  652ab0:	test   %rdi,%rdi
  652ab3:	mov    0x88(%rsp),%r8
  652abb:	mov    0xb8(%rsp),%rsi
  652ac3:	je     652520 <valar_spiral_rs::poly::from_ntt+0x710>
  652ac9:	vpbroadcastq %xmm0,%ymm0
  652ace:	vpbroadcastq %xmm1,%ymm1
  652ad3:	xor    %r15d,%r15d
  652ad6:	mov    0x158(%rsp),%rax
  652ade:	xchg   %ax,%ax
  652ae0:	cmp    %rdi,%r15
  652ae3:	jae    65334c <valar_spiral_rs::poly::from_ntt+0x153c>
  652ae9:	vmovdqa (%r8,%r15,8),%ymm2
  652aef:	vpcmpgtq %ymm2,%ymm0,%ymm3
  652af4:	vpandn %ymm0,%ymm3,%ymm3
  652af8:	vpsubq %ymm3,%ymm2,%ymm2
  652afc:	vpcmpgtq %ymm2,%ymm1,%ymm3
  652b01:	vpandn %ymm1,%ymm3,%ymm3
  652b05:	vpsubq %ymm3,%ymm2,%ymm2
  652b09:	vmovdqa %ymm2,(%r8,%r15,8)
  652b0f:	add    $0x4,%r15
  652b13:	dec    %rax
  652b16:	jne    652ae0 <valar_spiral_rs::poly::from_ntt+0xcd0>
  652b18:	jmp    652520 <valar_spiral_rs::poly::from_ntt+0x710>
  652b1d:	mov    0x18(%rsp),%rsi
  652b22:	test   %rsi,%rsi
  652b25:	je     653040 <valar_spiral_rs::poly::from_ntt+0x1230>
  652b2b:	cmp    $0x3,%rsi
  652b2f:	ja     652b8a <valar_spiral_rs::poly::from_ntt+0xd7a>
  652b31:	xor    %eax,%eax
  652b33:	jmp    653085 <valar_spiral_rs::poly::from_ntt+0x1275>
  652b38:	test   %r8,%r8
  652b3b:	mov    0x30(%rsp),%rdx
  652b40:	je     652ba3 <valar_spiral_rs::poly::from_ntt+0xd93>
  652b42:	cmp    %rdx,%r8
  652b45:	ja     6532b1 <valar_spiral_rs::poly::from_ntt+0x14a1>
  652b4b:	test   %rdi,%rdi
  652b4e:	je     653405 <valar_spiral_rs::poly::from_ntt+0x15f5>
  652b54:	mov    0x10(%rax),%rsi
  652b58:	cmp    $0x3,%rsi
  652b5c:	jb     65341a <valar_spiral_rs::poly::from_ntt+0x160a>
  652b62:	mov    0x18(%rsp),%rdi
  652b67:	je     6533cc <valar_spiral_rs::poly::from_ntt+0x15bc>
  652b6d:	mov    0x10(%rsp),%rax
  652b72:	mov    0xa8(%rax),%rax
  652b79:	lea    (%rax,%rax,1),%rcx
  652b7d:	cmp    $0x4,%rdi
  652b81:	jae    652bc5 <valar_spiral_rs::poly::from_ntt+0xdb5>
  652b83:	xor    %edx,%edx
  652b85:	jmp    652ee5 <valar_spiral_rs::poly::from_ntt+0x10d5>
  652b8a:	vmovq  0x28(%rsp),%xmm0
  652b90:	vmovq  0x48(%rsp),%xmm1
  652b96:	cmp    $0x10,%rsi
  652b9a:	jae    652be0 <valar_spiral_rs::poly::from_ntt+0xdd0>
  652b9c:	xor    %eax,%eax
  652b9e:	jmp    652ce6 <valar_spiral_rs::poly::from_ntt+0xed6>
  652ba3:	test   %rdi,%rdi
  652ba6:	je     653405 <valar_spiral_rs::poly::from_ntt+0x15f5>
  652bac:	mov    0x10(%rax),%rsi
  652bb0:	cmp    $0x3,%rsi
  652bb4:	jb     65341a <valar_spiral_rs::poly::from_ntt+0x160a>
  652bba:	jne    653040 <valar_spiral_rs::poly::from_ntt+0x1230>
  652bc0:	jmp    6533cc <valar_spiral_rs::poly::from_ntt+0x15bc>
  652bc5:	vmovq  %rax,%xmm0
  652bca:	vmovq  %rcx,%xmm1
  652bcf:	cmp    $0x10,%rdi
  652bd3:	jae    652d52 <valar_spiral_rs::poly::from_ntt+0xf42>
  652bd9:	xor    %edx,%edx
  652bdb:	jmp    652e62 <valar_spiral_rs::poly::from_ntt+0x1052>
  652be0:	mov    %rsi,%rax
  652be3:	and    $0xfffffffffffffff0,%rax
  652be7:	vpbroadcastq %xmm0,%ymm2
  652bec:	vpbroadcastq %xmm1,%ymm3
  652bf1:	xor    %ecx,%ecx
  652bf3:	vmovdqu 0x160(%rsp),%ymm13
  652bfc:	nopl   0x0(%rax)
  652c00:	vmovdqu (%rbx,%rcx,8),%ymm4
  652c05:	vmovdqu 0x20(%rbx,%rcx,8),%ymm5
  652c0b:	vmovdqu 0x40(%rbx,%rcx,8),%ymm6
  652c11:	vmovdqu 0x60(%rbx,%rcx,8),%ymm7
  652c17:	vpxor  %ymm2,%ymm13,%ymm8
  652c1b:	vpxor  %ymm4,%ymm13,%ymm9
  652c1f:	vpcmpgtq %ymm9,%ymm8,%ymm9
  652c24:	vpandn %ymm2,%ymm9,%ymm9
  652c28:	vpxor  %ymm5,%ymm13,%ymm10
  652c2c:	vpcmpgtq %ymm10,%ymm8,%ymm10
  652c31:	vpandn %ymm2,%ymm10,%ymm10
  652c35:	vpxor  %ymm6,%ymm13,%ymm11
  652c39:	vpcmpgtq %ymm11,%ymm8,%ymm11
  652c3e:	vpandn %ymm2,%ymm11,%ymm11
  652c42:	vpxor  %ymm7,%ymm13,%ymm12
  652c46:	vpcmpgtq %ymm12,%ymm8,%ymm8
  652c4b:	vpandn %ymm2,%ymm8,%ymm8
  652c4f:	vpsubq %ymm9,%ymm4,%ymm4
  652c54:	vpsubq %ymm10,%ymm5,%ymm5
  652c59:	vpsubq %ymm11,%ymm6,%ymm6
  652c5e:	vpsubq %ymm8,%ymm7,%ymm7
  652c63:	vpxor  %ymm4,%ymm13,%ymm8
  652c67:	vpxor  %ymm3,%ymm13,%ymm9
  652c6b:	vpcmpgtq %ymm8,%ymm9,%ymm8
  652c70:	vpandn %ymm3,%ymm8,%ymm8
  652c74:	vpxor  %ymm5,%ymm13,%ymm10
  652c78:	vpcmpgtq %ymm10,%ymm9,%ymm10
  652c7d:	vpandn %ymm3,%ymm10,%ymm10
  652c81:	vpxor  %ymm6,%ymm13,%ymm11
  652c85:	vpcmpgtq %ymm11,%ymm9,%ymm11
  652c8a:	vpandn %ymm3,%ymm11,%ymm11
  652c8e:	vpxor  %ymm7,%ymm13,%ymm12
  652c92:	vpcmpgtq %ymm12,%ymm9,%ymm9
  652c97:	vpandn %ymm3,%ymm9,%ymm9
  652c9b:	vpsubq %ymm8,%ymm4,%ymm4
  652ca0:	vpsubq %ymm10,%ymm5,%ymm5
  652ca5:	vpsubq %ymm11,%ymm6,%ymm6
  652caa:	vpsubq %ymm9,%ymm7,%ymm7
  652caf:	vmovdqu %ymm4,(%rbx,%rcx,8)
  652cb4:	vmovdqu %ymm5,0x20(%rbx,%rcx,8)
  652cba:	vmovdqu %ymm6,0x40(%rbx,%rcx,8)
  652cc0:	vmovdqu %ymm7,0x60(%rbx,%rcx,8)
  652cc6:	add    $0x10,%rcx
  652cca:	cmp    %rcx,%rax
  652ccd:	jne    652c00 <valar_spiral_rs::poly::from_ntt+0xdf0>
  652cd3:	cmp    %rax,%rsi
  652cd6:	je     653040 <valar_spiral_rs::poly::from_ntt+0x1230>
  652cdc:	test   $0xc,%sil
  652ce0:	je     653085 <valar_spiral_rs::poly::from_ntt+0x1275>
  652ce6:	mov    %rax,%rcx
  652ce9:	mov    %rsi,%rax
  652cec:	and    $0xfffffffffffffffc,%rax
  652cf0:	vpbroadcastq %xmm0,%ymm0
  652cf5:	vpbroadcastq %xmm1,%ymm1
  652cfa:	vmovdqu 0x160(%rsp),%ymm5
  652d03:	data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  652d10:	vmovdqu (%rbx,%rcx,8),%ymm2
  652d15:	vpxor  %ymm5,%ymm0,%ymm3
  652d19:	vpxor  %ymm5,%ymm2,%ymm4
  652d1d:	vpcmpgtq %ymm4,%ymm3,%ymm3
  652d22:	vpandn %ymm0,%ymm3,%ymm3
  652d26:	vpsubq %ymm3,%ymm2,%ymm2
  652d2a:	vpxor  %ymm5,%ymm2,%ymm3
  652d2e:	vpxor  %ymm5,%ymm1,%ymm4
  652d32:	vpcmpgtq %ymm3,%ymm4,%ymm3
  652d37:	vpandn %ymm1,%ymm3,%ymm3
  652d3b:	vpsubq %ymm3,%ymm2,%ymm2
  652d3f:	vmovdqu %ymm2,(%rbx,%rcx,8)
  652d44:	add    $0x4,%rcx
  652d48:	cmp    %rcx,%rax
  652d4b:	jne    652d10 <valar_spiral_rs::poly::from_ntt+0xf00>
  652d4d:	jmp    653080 <valar_spiral_rs::poly::from_ntt+0x1270>
  652d52:	mov    %rdi,%rdx
  652d55:	and    $0xfffffffffffffff0,%rdx
  652d59:	vpbroadcastq %xmm0,%ymm2
  652d5e:	vpbroadcastq %xmm1,%ymm3
  652d63:	vmovdqu 0x160(%rsp),%ymm14
  652d6c:	vpxor  %ymm3,%ymm14,%ymm4
  652d70:	vpxor  %ymm2,%ymm14,%ymm5
  652d74:	xor    %esi,%esi
  652d76:	cs nopw 0x0(%rax,%rax,1)
  652d80:	vmovdqu (%rbx,%rsi,8),%ymm6
  652d85:	vmovdqu 0x20(%rbx,%rsi,8),%ymm7
  652d8b:	vmovdqu 0x40(%rbx,%rsi,8),%ymm8
  652d91:	vmovdqu 0x60(%rbx,%rsi,8),%ymm9
  652d97:	vpxor  %ymm6,%ymm14,%ymm10
  652d9b:	vpcmpgtq %ymm10,%ymm4,%ymm10
  652da0:	vpandn %ymm3,%ymm10,%ymm10
  652da4:	vpxor  %ymm7,%ymm14,%ymm11
  652da8:	vpcmpgtq %ymm11,%ymm4,%ymm11
  652dad:	vpandn %ymm3,%ymm11,%ymm11
  652db1:	vpxor  %ymm14,%ymm8,%ymm12
  652db6:	vpcmpgtq %ymm12,%ymm4,%ymm12
  652dbb:	vpandn %ymm3,%ymm12,%ymm12
  652dbf:	vpxor  %ymm14,%ymm9,%ymm13
  652dc4:	vpcmpgtq %ymm13,%ymm4,%ymm13
  652dc9:	vpandn %ymm3,%ymm13,%ymm13
  652dcd:	vpsubq %ymm10,%ymm6,%ymm6
  652dd2:	vpsubq %ymm11,%ymm7,%ymm7
  652dd7:	vpsubq %ymm12,%ymm8,%ymm8
  652ddc:	vpsubq %ymm13,%ymm9,%ymm9
  652de1:	vpxor  %ymm6,%ymm14,%ymm10
  652de5:	vpcmpgtq %ymm10,%ymm5,%ymm10
  652dea:	vpandn %ymm2,%ymm10,%ymm10
  652dee:	vpxor  %ymm7,%ymm14,%ymm11
  652df2:	vpcmpgtq %ymm11,%ymm5,%ymm11
  652df7:	vpandn %ymm2,%ymm11,%ymm11
  652dfb:	vpxor  %ymm14,%ymm8,%ymm12
  652e00:	vpcmpgtq %ymm12,%ymm5,%ymm12
  652e05:	vpandn %ymm2,%ymm12,%ymm12
  652e09:	vpxor  %ymm14,%ymm9,%ymm13
  652e0e:	vpcmpgtq %ymm13,%ymm5,%ymm13
  652e13:	vpandn %ymm2,%ymm13,%ymm13
  652e17:	vpsubq %ymm10,%ymm6,%ymm6
  652e1c:	vpsubq %ymm11,%ymm7,%ymm7
  652e21:	vpsubq %ymm12,%ymm8,%ymm8
  652e26:	vpsubq %ymm13,%ymm9,%ymm9
  652e2b:	vmovdqu %ymm6,(%rbx,%rsi,8)
  652e30:	vmovdqu %ymm7,0x20(%rbx,%rsi,8)
  652e36:	vmovdqu %ymm8,0x40(%rbx,%rsi,8)
  652e3c:	vmovdqu %ymm9,0x60(%rbx,%rsi,8)
  652e42:	add    $0x10,%rsi
  652e46:	cmp    %rsi,%rdx
  652e49:	jne    652d80 <valar_spiral_rs::poly::from_ntt+0xf70>
  652e4f:	cmp    %rdx,%rdi
  652e52:	je     653040 <valar_spiral_rs::poly::from_ntt+0x1230>
  652e58:	test   $0xc,%dil
  652e5c:	je     652ee5 <valar_spiral_rs::poly::from_ntt+0x10d5>
  652e62:	mov    %rdx,%rsi
  652e65:	mov    %rdi,%rdx
  652e68:	and    $0xfffffffffffffffc,%rdx
  652e6c:	vpbroadcastq %xmm0,%ymm0
  652e71:	vpbroadcastq %xmm1,%ymm1
  652e76:	vmovdqu 0x160(%rsp),%ymm6
  652e7f:	vpxor  %ymm6,%ymm1,%ymm2
  652e83:	vpxor  %ymm6,%ymm0,%ymm3
  652e87:	nopw   0x0(%rax,%rax,1)
  652e90:	vmovdqu (%rbx,%rsi,8),%ymm4
  652e95:	vpxor  %ymm6,%ymm4,%ymm5
  652e99:	vpcmpgtq %ymm5,%ymm2,%ymm5
  652e9e:	vpandn %ymm1,%ymm5,%ymm5
  652ea2:	vpsubq %ymm5,%ymm4,%ymm4
  652ea6:	vpxor  %ymm6,%ymm4,%ymm5
  652eaa:	vpcmpgtq %ymm5,%ymm3,%ymm5
  652eaf:	vpandn %ymm0,%ymm5,%ymm5
  652eb3:	vpsubq %ymm5,%ymm4,%ymm4
  652eb7:	vmovdqu %ymm4,(%rbx,%rsi,8)
  652ebc:	add    $0x4,%rsi
  652ec0:	cmp    %rsi,%rdx
  652ec3:	jne    652e90 <valar_spiral_rs::poly::from_ntt+0x1080>
  652ec5:	cmp    %rdx,%rdi
  652ec8:	jne    652ee5 <valar_spiral_rs::poly::from_ntt+0x10d5>
  652eca:	jmp    653040 <valar_spiral_rs::poly::from_ntt+0x1230>
  652ecf:	nop
  652ed0:	sub    %rdi,%rsi
  652ed3:	mov    %rsi,(%rbx,%rdx,8)
  652ed7:	inc    %rdx
  652eda:	cmp    %rdx,0x18(%rsp)
  652edf:	je     653040 <valar_spiral_rs::poly::from_ntt+0x1230>
  652ee5:	mov    (%rbx,%rdx,8),%rsi
  652ee9:	mov    $0x0,%edi
  652eee:	cmp    %rcx,%rsi
  652ef1:	jb     652ef6 <valar_spiral_rs::poly::from_ntt+0x10e6>
  652ef3:	mov    %rcx,%rdi
  652ef6:	sub    %rdi,%rsi
  652ef9:	mov    $0x0,%edi
  652efe:	cmp    %rax,%rsi
  652f01:	jb     652ed0 <valar_spiral_rs::poly::from_ntt+0x10c0>
  652f03:	mov    %rax,%rdi
  652f06:	jmp    652ed0 <valar_spiral_rs::poly::from_ntt+0x10c0>
  652f08:	nopl   0x0(%rax,%rax,1)
  652f10:	mov    0xa8(%rsp),%rax
  652f18:	mov    (%rax),%rax
  652f1b:	cmp    $0x1,%rax
  652f1f:	jne    652f40 <valar_spiral_rs::poly::from_ntt+0x1130>
  652f21:	cmp    %rcx,%r9
  652f24:	jae    6533a2 <valar_spiral_rs::poly::from_ntt+0x1592>
  652f2a:	mov    (%rbx,%r9,8),%rax
  652f2e:	jmp    65300c <valar_spiral_rs::poly::from_ntt+0x11fc>
  652f33:	data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  652f40:	cmp    %rcx,%r9
  652f43:	jae    65339d <valar_spiral_rs::poly::from_ntt+0x158d>
  652f49:	mov    0x10(%rsp),%rdx
  652f4e:	mov    0x30(%rdx),%rdi
  652f52:	add    %r9,%rdi
  652f55:	cmp    %rcx,%rdi
  652f58:	jae    6533ae <valar_spiral_rs::poly::from_ntt+0x159e>
  652f5e:	cmp    $0x2,%rax
  652f62:	jne    65315d <valar_spiral_rs::poly::from_ntt+0x134d>
  652f68:	mov    (%rbx,%r9,8),%rdx
  652f6c:	mov    (%rbx,%rdi,8),%rax
  652f70:	mov    0x10(%rsp),%r11
  652f75:	mulx   0xa0(%r11),%r10,%rdi
  652f7e:	mov    %rax,%rdx
  652f81:	mulx   0x98(%r11),%rax,%rcx
  652f8a:	add    %r10,%rax
  652f8d:	adc    %rdi,%rcx
  652f90:	mov    0x88(%r11),%r14
  652f97:	mov    0x90(%r11),%rdi
  652f9e:	mov    %rax,%rdx
  652fa1:	mulx   %r14,%r11,%r11
  652fa6:	mulx   %rdi,%rbx,%r10
  652fab:	mov    %rcx,%rdx
  652fae:	mulx   %r14,%r15,%r14
  652fb3:	mov    $0x0,%edx
  652fb8:	add    %rbx,%r11
  652fbb:	jb     652fcd <valar_spiral_rs::poly::from_ntt+0x11bd>
  652fbd:	mov    %r11,%r12
  652fc0:	not    %r12
  652fc3:	cmp    %r15,%r12
  652fc6:	mov    0x8(%rsp),%r12
  652fcb:	jb     65302f <valar_spiral_rs::poly::from_ntt+0x121f>
  652fcd:	mov    0x10(%rsp),%r15
  652fd2:	mov    0xc8(%r15),%r15
  652fd9:	imul   %rcx,%rdi
  652fdd:	add    %r14,%rdi
  652fe0:	cmp    %rbx,%r11
  652fe3:	adc    %r10,%rdi
  652fe6:	add    %rdx,%rdi
  652fe9:	imul   %r15,%rdi
  652fed:	sub    %rdi,%rax
  652ff0:	cmp    %r15,%rax
  652ff3:	mov    $0x0,%ecx
  652ff8:	cmovae %r15,%rcx
  652ffc:	sub    %rcx,%rax
  652fff:	mov    0x30(%rsp),%rcx
  653004:	mov    0xc0(%rsp),%rbx
  65300c:	cmp    %r9,0xb0(%rsp)
  653014:	je     653389 <valar_spiral_rs::poly::from_ntt+0x1579>
  65301a:	mov    %rax,(%r8,%r9,8)
  65301e:	inc    %r9
  653021:	cmp    %r9,%rsi
  653024:	jne    652f10 <valar_spiral_rs::poly::from_ntt+0x1100>
  65302a:	jmp    651f50 <valar_spiral_rs::poly::from_ntt+0x140>
  65302f:	mov    $0x1,%edx
  653034:	jmp    652fcd <valar_spiral_rs::poly::from_ntt+0x11bd>
  653036:	cs nopw 0x0(%rax,%rax,1)
  653040:	mov    0x10(%rsp),%rax
  653045:	mov    0x30(%rax),%rsi
  653049:	test   %rsi,%rsi
  65304c:	mov    0x30(%rsp),%rcx
  653051:	je     651f50 <valar_spiral_rs::poly::from_ntt+0x140>
  653057:	mov    0xb0(%rsp),%r8
  65305f:	imul   0xf0(%rsp),%r8
  653068:	add    0x118(%rsp),%r8
  653070:	xor    %r9d,%r9d
  653073:	jmp    652f10 <valar_spiral_rs::poly::from_ntt+0x1100>
  653078:	nopl   0x0(%rax,%rax,1)
  653080:	cmp    %rax,%rsi
  653083:	je     653040 <valar_spiral_rs::poly::from_ntt+0x1230>
  653085:	mov    (%rbx,%rax,8),%rcx
  653089:	mov    $0x0,%edx
  65308e:	cmp    0x28(%rsp),%rcx
  653093:	jb     65309a <valar_spiral_rs::poly::from_ntt+0x128a>
  653095:	mov    0x28(%rsp),%rdx
  65309a:	sub    %rdx,%rcx
  65309d:	mov    $0x0,%edx
  6530a2:	cmp    0x48(%rsp),%rcx
  6530a7:	jb     6530ae <valar_spiral_rs::poly::from_ntt+0x129e>
  6530a9:	mov    0x48(%rsp),%rdx
  6530ae:	sub    %rdx,%rcx
  6530b1:	mov    %rcx,(%rbx,%rax,8)
  6530b5:	inc    %rax
  6530b8:	jmp    653080 <valar_spiral_rs::poly::from_ntt+0x1270>
  6530ba:	mov    0x108(%rsp),%rax
  6530c2:	add    0xf8(%rsp),%rax
  6530ca:	cmp    0x100(%rsp),%rdi
  6530d2:	jne    651f21 <valar_spiral_rs::poly::from_ntt+0x111>
  6530d8:	mov    (%r12),%rax
  6530dc:	inc    %rax
  6530df:	jmp    6530e3 <valar_spiral_rs::poly::from_ntt+0x12d3>
  6530e1:	xor    %eax,%eax
  6530e3:	mov    %rax,(%r12)
  6530e7:	add    $0x1b8,%rsp
  6530ee:	pop    %rbx
  6530ef:	pop    %r12
  6530f1:	pop    %r13
  6530f3:	pop    %r14
  6530f5:	pop    %r15
  6530f7:	pop    %rbp
  6530f8:	vzeroupper
  6530fb:	ret
  6530fc:	lea    0x82a05(%rip),%rsi        # 6d5b08 <tokio::runtime::task::waker::WAKER_VTABLE+0x2570>
  653103:	lea    0x188(%rsp),%rdi
  65310b:	lea    0x82e16(%rip),%rax        # 6d5f28 <tokio::runtime::task::waker::WAKER_VTABLE+0x2990>
  653112:	mov    %rax,(%rdi)
  653115:	vmovdqa -0x5d77dd(%rip),%ymm0        # 7b940 <GCC_except_table6770+0x36f0>
  65311d:	vmovdqu %ymm0,0x8(%rdi)
  653122:	vzeroupper
  653125:	call   288420 <core::panicking::panic_fmt>
  65312a:	jmp    6533ca <valar_spiral_rs::poly::from_ntt+0x15ba>
  65312f:	lea    0x82a4a(%rip),%rdx        # 6d5b80 <tokio::runtime::task::waker::WAKER_VTABLE+0x25e8>
  653136:	mov    %rbp,%rsi
  653139:	jmp    653148 <valar_spiral_rs::poly::from_ntt+0x1338>
  65313b:	mov    %rdi,%r15
  65313e:	mov    %rbp,%rsi
  653141:	lea    0x82a20(%rip),%rdx        # 6d5b68 <tokio::runtime::task::waker::WAKER_VTABLE+0x25d0>
  653148:	mov    0x8(%rsp),%r12
  65314d:	mov    %r15,%rdi
  653150:	vzeroupper
  653153:	call   28beb0 <core::panicking::panic_bounds_check>
  653158:	jmp    6533ca <valar_spiral_rs::poly::from_ntt+0x15ba>
  65315d:	movq   $0x0,0x188(%rsp)
  653169:	lea    -0x5d6c90(%rip),%rdx        # 7c4e0 <GCC_except_table6770+0x4290>
  653170:	lea    0x82e21(%rip),%r8        # 6d5f98 <tokio::runtime::task::waker::WAKER_VTABLE+0x2a00>
  653177:	lea    0x188(%rsp),%rcx
  65317f:	xor    %edi,%edi
  653181:	mov    0xa8(%rsp),%rsi
  653189:	vzeroupper
  65318c:	call   290b5a <core::panicking::assert_failed>
  653191:	jmp    6533ca <valar_spiral_rs::poly::from_ntt+0x15ba>
  653196:	mov    %r12,%r15
  653199:	mov    %r8,%rsi
  65319c:	lea    0x828bd(%rip),%rdx        # 6d5a60 <tokio::runtime::task::waker::WAKER_VTABLE+0x24c8>
  6531a3:	jmp    653148 <valar_spiral_rs::poly::from_ntt+0x1338>
  6531a5:	lea    0x828cc(%rip),%rdx        # 6d5a78 <tokio::runtime::task::waker::WAKER_VTABLE+0x24e0>
  6531ac:	mov    %r8,%rsi
  6531af:	jmp    653148 <valar_spiral_rs::poly::from_ntt+0x1338>
  6531b1:	lea    0x82848(%rip),%rsi        # 6d5a00 <tokio::runtime::task::waker::WAKER_VTABLE+0x2468>
  6531b8:	jmp    653103 <valar_spiral_rs::poly::from_ntt+0x12f3>
  6531bd:	mov    %rcx,0x18(%rsp)
  6531c2:	lea    0x829e7(%rip),%rcx        # 6d5bb0 <tokio::runtime::task::waker::WAKER_VTABLE+0x2618>
  6531c9:	mov    0x30(%rsp),%rdx
  6531ce:	jmp    6531dc <valar_spiral_rs::poly::from_ntt+0x13cc>
  6531d0:	mov    %rcx,0x18(%rsp)
  6531d5:	lea    0x829d4(%rip),%rcx        # 6d5bb0 <tokio::runtime::task::waker::WAKER_VTABLE+0x2618>
  6531dc:	mov    %rax,%rdi
  6531df:	mov    0x18(%rsp),%rsi
  6531e4:	vzeroupper
  6531e7:	call   289700 <core::slice::index::slice_index_fail>
  6531ec:	jmp    6533ca <valar_spiral_rs::poly::from_ntt+0x15ba>
  6531f1:	lea    0x82958(%rip),%rdi        # 6d5b50 <tokio::runtime::task::waker::WAKER_VTABLE+0x25b8>
  6531f8:	jmp    653206 <valar_spiral_rs::poly::from_ntt+0x13f6>
  6531fa:	lea    0x8294f(%rip),%rdi        # 6d5b50 <tokio::runtime::task::waker::WAKER_VTABLE+0x25b8>
  653201:	mov    0x8(%rsp),%r12
  653206:	vzeroupper
  653209:	call   288440 <core::option::unwrap_failed>
  65320e:	jmp    6533ca <valar_spiral_rs::poly::from_ntt+0x15ba>
  653213:	mov    %r10,%rsi
  653216:	lea    0x82903(%rip),%rdx        # 6d5b20 <tokio::runtime::task::waker::WAKER_VTABLE+0x2588>
  65321d:	jmp    653148 <valar_spiral_rs::poly::from_ntt+0x1338>
  653222:	lea    0x8290f(%rip),%rdx        # 6d5b38 <tokio::runtime::task::waker::WAKER_VTABLE+0x25a0>
  653229:	mov    0x8(%rsp),%r12
  65322e:	mov    0x40(%rsp),%rsi
  653233:	jmp    65314d <valar_spiral_rs::poly::from_ntt+0x133d>
  653238:	lea    0x82be1(%rip),%rcx        # 6d5e20 <tokio::runtime::task::waker::WAKER_VTABLE+0x2888>
  65323f:	xor    %edi,%edi
  653241:	mov    %rdx,%rsi
  653244:	mov    0x30(%rsp),%rdx
  653249:	vzeroupper
  65324c:	call   289700 <core::slice::index::slice_index_fail>
  653251:	jmp    6533ca <valar_spiral_rs::poly::from_ntt+0x15ba>
  653256:	add    %rax,%r8
  653259:	mov    %rdi,%rsi
  65325c:	mov    %r8,%r15
  65325f:	lea    0x828d2(%rip),%rdx        # 6d5b38 <tokio::runtime::task::waker::WAKER_VTABLE+0x25a0>
  653266:	jmp    65314d <valar_spiral_rs::poly::from_ntt+0x133d>
  65326b:	cmp    %rax,%r10
  65326e:	cmova  %r10,%rax
  653272:	mov    %rax,%r15
  653275:	mov    %r10,%rsi
  653278:	lea    0x828a1(%rip),%rdx        # 6d5b20 <tokio::runtime::task::waker::WAKER_VTABLE+0x2588>
  65327f:	jmp    65314d <valar_spiral_rs::poly::from_ntt+0x133d>
  653284:	cmp    %rax,%rdi
  653287:	cmova  %rdi,%rax
  65328b:	mov    %rdi,%rsi
  65328e:	mov    %rax,%r15
  653291:	lea    0x828a0(%rip),%rdx        # 6d5b38 <tokio::runtime::task::waker::WAKER_VTABLE+0x25a0>
  653298:	jmp    65314d <valar_spiral_rs::poly::from_ntt+0x133d>
  65329d:	mov    0x48(%rsp),%r15
  6532a2:	mov    %rbp,%rsi
  6532a5:	lea    0x828ec(%rip),%rdx        # 6d5b98 <tokio::runtime::task::waker::WAKER_VTABLE+0x2600>
  6532ac:	jmp    653148 <valar_spiral_rs::poly::from_ntt+0x1338>
  6532b1:	xor    %eax,%eax
  6532b3:	lea    0x827d6(%rip),%rcx        # 6d5a90 <tokio::runtime::task::waker::WAKER_VTABLE+0x24f8>
  6532ba:	jmp    6531dc <valar_spiral_rs::poly::from_ntt+0x13cc>
  6532bf:	mov    0x90(%rsp),%r15
  6532c7:	mov    %rbp,%rsi
  6532ca:	lea    0x828c7(%rip),%rdx        # 6d5b98 <tokio::runtime::task::waker::WAKER_VTABLE+0x2600>
  6532d1:	jmp    65314d <valar_spiral_rs::poly::from_ntt+0x133d>
  6532d6:	mov    %fs:0x0,%rax
  6532df:	lea    -0x80(%rax),%rax
  6532e6:	mov    %rdi,%rbx
  6532e9:	mov    %rax,%rdi
  6532ec:	mov    %rsi,%r14
  6532ef:	call   653450 <std::sys::thread_local::native::lazy::Storage<T,D>::get_or_init_slow>
  6532f4:	mov    %r14,%rsi
  6532f7:	mov    %rbx,%rdi
  6532fa:	mov    %rax,%r12
  6532fd:	test   %rax,%rax
  653300:	jne    651e49 <valar_spiral_rs::poly::from_ntt+0x39>
  653306:	lea    0x82d4b(%rip),%rdi        # 6d6058 <tokio::runtime::task::waker::WAKER_VTABLE+0x2ac0>
  65330d:	call   4b9410 <std::thread::local::panic_access_error>
  653312:	lea    0x82b1f(%rip),%rdi        # 6d5e38 <tokio::runtime::task::waker::WAKER_VTABLE+0x28a0>
  653319:	call   293140 <core::cell::panic_already_borrowed>
  65331e:	lea    0x82723(%rip),%rdi        # 6d5a48 <tokio::runtime::task::waker::WAKER_VTABLE+0x24b0>
  653325:	jmp    653206 <valar_spiral_rs::poly::from_ntt+0x13f6>
  65332a:	mov    0x38(%rsp),%rsi
  65332f:	lea    0x826fa(%rip),%rdx        # 6d5a30 <tokio::runtime::task::waker::WAKER_VTABLE+0x2498>
  653336:	jmp    65314d <valar_spiral_rs::poly::from_ntt+0x133d>
  65333b:	lea    0x826d6(%rip),%rdx        # 6d5a18 <tokio::runtime::task::waker::WAKER_VTABLE+0x2480>
  653342:	mov    0x40(%rsp),%rsi
  653347:	jmp    65314d <valar_spiral_rs::poly::from_ntt+0x133d>
  65334c:	mov    %rdi,%rsi
  65334f:	lea    0x8279a(%rip),%rdx        # 6d5af0 <tokio::runtime::task::waker::WAKER_VTABLE+0x2558>
  653356:	jmp    65314d <valar_spiral_rs::poly::from_ntt+0x133d>
  65335b:	mov    0x38(%rsp),%rsi
  653360:	cmp    %r15,%rsi
  653363:	cmova  %rsi,%r15
  653367:	lea    0x826c2(%rip),%rdx        # 6d5a30 <tokio::runtime::task::waker::WAKER_VTABLE+0x2498>
  65336e:	jmp    65314d <valar_spiral_rs::poly::from_ntt+0x133d>
  653373:	cmp    %r15,%rdi
  653376:	cmova  %rdi,%r15
  65337a:	mov    %rdi,%rsi
  65337d:	lea    0x82694(%rip),%rdx        # 6d5a18 <tokio::runtime::task::waker::WAKER_VTABLE+0x2480>
  653384:	jmp    65314d <valar_spiral_rs::poly::from_ntt+0x133d>
  653389:	mov    0xb0(%rsp),%rcx
  653391:	mov    %rcx,%rdi
  653394:	lea    0x82a6d(%rip),%rax        # 6d5e08 <tokio::runtime::task::waker::WAKER_VTABLE+0x2870>
  65339b:	jmp    6533b5 <valar_spiral_rs::poly::from_ntt+0x15a5>
  65339d:	mov    %r9,%rdi
  6533a0:	jmp    6533ba <valar_spiral_rs::poly::from_ntt+0x15aa>
  6533a2:	mov    %r9,%rdi
  6533a5:	lea    0x82ba4(%rip),%rax        # 6d5f50 <tokio::runtime::task::waker::WAKER_VTABLE+0x29b8>
  6533ac:	jmp    6533b5 <valar_spiral_rs::poly::from_ntt+0x15a5>
  6533ae:	lea    0x82bcb(%rip),%rax        # 6d5f80 <tokio::runtime::task::waker::WAKER_VTABLE+0x29e8>
  6533b5:	mov    %rax,0x60(%rsp)
  6533ba:	mov    %rcx,%rsi
  6533bd:	mov    0x60(%rsp),%rdx
  6533c2:	vzeroupper
  6533c5:	call   28beb0 <core::panicking::panic_bounds_check>
  6533ca:	ud2
  6533cc:	mov    $0x3,%r15d
  6533d2:	mov    $0x3,%esi
  6533d7:	lea    0x82c4a(%rip),%rdx        # 6d6028 <tokio::runtime::task::waker::WAKER_VTABLE+0x2a90>
  6533de:	jmp    65314d <valar_spiral_rs::poly::from_ntt+0x133d>
  6533e3:	mov    0x68(%rsp),%rsi
  6533e8:	lea    0x82bf1(%rip),%rdx        # 6d5fe0 <tokio::runtime::task::waker::WAKER_VTABLE+0x2a48>
  6533ef:	jmp    65314d <valar_spiral_rs::poly::from_ntt+0x133d>
  6533f4:	mov    $0x4,%esi
  6533f9:	lea    0x826d8(%rip),%rdx        # 6d5ad8 <tokio::runtime::task::waker::WAKER_VTABLE+0x2540>
  653400:	jmp    65314d <valar_spiral_rs::poly::from_ntt+0x133d>
  653405:	mov    %rdi,%rsi
  653408:	xor    %r15d,%r15d
  65340b:	lea    0x82bce(%rip),%rdx        # 6d5fe0 <tokio::runtime::task::waker::WAKER_VTABLE+0x2a48>
  653412:	jmp    65314d <valar_spiral_rs::poly::from_ntt+0x133d>
  653417:	mov    %rcx,%rsi
  65341a:	mov    $0x2,%r15d
  653420:	lea    0x82bd1(%rip),%rdx        # 6d5ff8 <tokio::runtime::task::waker::WAKER_VTABLE+0x2a60>
  653427:	jmp    65314d <valar_spiral_rs::poly::from_ntt+0x133d>
  65342c:	incq   (%r12)
  653430:	mov    %rax,%rdi
  653433:	call   6a51d0 <_Unwind_Resume@plt>
  653438:	incq   (%r12)
  65343c:	mov    %rax,%rdi
  65343f:	call   6a51d0 <_Unwind_Resume@plt>
