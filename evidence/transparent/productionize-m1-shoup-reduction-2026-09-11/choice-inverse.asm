
/opt/transparent-publisher-build/shoup-choice-20260911/artifacts/worker-integration-tests:     file format elf64-x86-64


Disassembly of section .text:

0000000000651c70 <valar_spiral_rs::poly::from_ntt>:
  651c70:	push   %rbp
  651c71:	push   %r15
  651c73:	push   %r14
  651c75:	push   %r13
  651c77:	push   %r12
  651c79:	push   %rbx
  651c7a:	sub    $0x1b8,%rsp
  651c81:	mov    0x20(%rdi),%rax
  651c85:	mov    %rax,0x8(%rsp)
  651c8a:	cmpb   $0x1,%fs:0xffffffffffffffa8
  651c93:	jne    653004 <valar_spiral_rs::poly::from_ntt+0x1394>
  651c99:	mov    %fs:0x0,%rax
  651ca2:	lea    -0x80(%rax),%rbp
  651ca9:	cmpq   $0x0,0x0(%rbp)
  651cae:	jne    653040 <valar_spiral_rs::poly::from_ntt+0x13d0>
  651cb4:	movq   $0xffffffffffffffff,0x0(%rbp)
  651cbc:	mov    0x28(%rdi),%rax
  651cc0:	test   %rax,%rax
  651cc3:	je     652e31 <valar_spiral_rs::poly::from_ntt+0x11c1>
  651cc9:	mov    0x30(%rdi),%r8
  651ccd:	test   %r8,%r8
  651cd0:	je     652e31 <valar_spiral_rs::poly::from_ntt+0x11c1>
  651cd6:	mov    %rax,0xf0(%rsp)
  651cde:	mov    0x18(%rbp),%rbx
  651ce2:	mov    0x20(%rbp),%rcx
  651ce6:	mov    0x8(%rsp),%rax
  651ceb:	add    $0x40,%rax
  651cef:	mov    %rax,0x90(%rsp)
  651cf7:	mov    0x10(%rsi),%rax
  651cfb:	mov    %rax,0x128(%rsp)
  651d03:	mov    0x20(%rsi),%rax
  651d07:	mov    %rax,0x130(%rsp)
  651d0f:	mov    0x30(%rsi),%rsi
  651d13:	mov    0x10(%rdi),%rax
  651d17:	mov    %rax,0x108(%rsp)
  651d1f:	lea    0x10(%rbx),%rax
  651d23:	mov    %rax,0x100(%rsp)
  651d2b:	lea    0x0(,%r8,8),%rax
  651d33:	mov    %rax,0xe8(%rsp)
  651d3b:	lea    0x83f66(%rip),%rax        # 6d5ca8 <tokio::runtime::task::waker::WAKER_VTABLE+0x29d0>
  651d42:	mov    %rax,0x60(%rsp)
  651d47:	vpbroadcastq -0x5d5930(%rip),%ymm0        # 7c420 <GCC_except_table6769+0x41d0>
  651d50:	xor    %eax,%eax
  651d52:	xor    %edi,%edi
  651d54:	mov    %rbp,0x18(%rsp)
  651d59:	mov    %rbx,0xb0(%rsp)
  651d61:	mov    %rcx,0x38(%rsp)
  651d66:	mov    %r8,0x118(%rsp)
  651d6e:	mov    %rsi,0x110(%rsp)
  651d76:	vmovdqu %ymm0,0x160(%rsp)
  651d7f:	mov    %rsi,%rdx
  651d82:	imul   %rdi,%rdx
  651d86:	mov    %rdx,0x138(%rsp)
  651d8e:	inc    %rdi
  651d91:	mov    %rax,0xf8(%rsp)
  651d99:	mov    %rax,0xd8(%rsp)
  651da1:	xor    %r9d,%r9d
  651da4:	mov    %rdi,0x120(%rsp)
  651dac:	jmp    651de5 <valar_spiral_rs::poly::from_ntt+0x175>
  651dae:	xchg   %ax,%ax
  651db0:	mov    0x140(%rsp),%r9
  651db8:	inc    %r9
  651dbb:	addq   $0x8,0xd8(%rsp)
  651dc4:	mov    0x118(%rsp),%r8
  651dcc:	cmp    %r8,%r9
  651dcf:	mov    0x110(%rsp),%rsi
  651dd7:	mov    0x120(%rsp),%rdi
  651ddf:	je     652e0a <valar_spiral_rs::poly::from_ntt+0x119a>
  651de5:	mov    0x130(%rsp),%rax
  651ded:	mov    0x40(%rax),%rdx
  651df1:	imul   0x30(%rax),%rdx
  651df6:	cmp    %rcx,%rdx
  651df9:	ja     652f66 <valar_spiral_rs::poly::from_ntt+0x12f6>
  651dff:	mov    0x8(%rsp),%rax
  651e04:	mov    0x30(%rax),%rax
  651e08:	mov    %rax,0x98(%rsp)
  651e10:	mov    0x138(%rsp),%rax
  651e18:	mov    %r9,0x140(%rsp)
  651e20:	add    %r9,%rax
  651e23:	imul   %rdx,%rax
  651e27:	mov    0x128(%rsp),%rcx
  651e2f:	lea    (%rcx,%rax,8),%rsi
  651e33:	shl    $0x3,%rdx
  651e37:	mov    %rbx,%rdi
  651e3a:	vzeroupper
  651e3d:	call   *0x8573d(%rip)        # 6d7580 <memcpy@GLIBC_2.14>
  651e43:	vpxor  %xmm11,%xmm11,%xmm11
  651e48:	mov    0x90(%rsp),%rax
  651e50:	mov    (%rax),%rax
  651e53:	mov    %rax,0x10(%rsp)
  651e58:	test   %rax,%rax
  651e5b:	je     652d90 <valar_spiral_rs::poly::from_ntt+0x1120>
  651e61:	cmpq   $0x1,0x10(%rsp)
  651e67:	jne    6521e0 <valar_spiral_rs::poly::from_ntt+0x570>
  651e6d:	mov    0x8(%rsp),%rcx
  651e72:	mov    0x30(%rcx),%r8
  651e76:	mov    0x8(%rcx),%rax
  651e7a:	mov    0x10(%rcx),%rdi
  651e7e:	mov    0x38(%rcx),%rdx
  651e82:	mov    %rdx,%rcx
  651e85:	sub    $0x1,%rcx
  651e89:	mov    $0x0,%esi
  651e8e:	cmovae %rcx,%rsi
  651e92:	test   %rdx,%rdx
  651e95:	mov    %r8,0x10(%rsp)
  651e9a:	je     652888 <valar_spiral_rs::poly::from_ntt+0xc18>
  651ea0:	mov    %rdx,%r9
  651ea3:	mov    0x38(%rsp),%rdx
  651ea8:	cmp    %rdx,%r8
  651eab:	ja     652fdf <valar_spiral_rs::poly::from_ntt+0x136f>
  651eb1:	test   %rdi,%rdi
  651eb4:	je     653136 <valar_spiral_rs::poly::from_ntt+0x14c6>
  651eba:	mov    0x10(%rax),%rcx
  651ebe:	cmp    $0x3,%rcx
  651ec2:	jb     653148 <valar_spiral_rs::poly::from_ntt+0x14d8>
  651ec8:	je     6530fa <valar_spiral_rs::poly::from_ntt+0x148a>
  651ece:	mov    0x8(%rax),%rax
  651ed2:	mov    0x38(%rax),%rcx
  651ed6:	mov    %rcx,0xa0(%rsp)
  651ede:	mov    0x40(%rax),%rdi
  651ee2:	mov    0x50(%rax),%rcx
  651ee6:	mov    %rcx,0x70(%rsp)
  651eeb:	mov    0x58(%rax),%rax
  651eef:	mov    %rax,0x48(%rsp)
  651ef4:	mov    0x8(%rsp),%rax
  651ef9:	mov    0xa8(%rax),%rax
  651f00:	mov    %rax,0x30(%rsp)
  651f05:	add    %rax,%rax
  651f08:	mov    %rax,0x28(%rsp)
  651f0d:	mov    %r9,%rcx
  651f10:	mov    %rdi,0x40(%rsp)
  651f15:	jmp    651f32 <valar_spiral_rs::poly::from_ntt+0x2c2>
  651f17:	nopw   0x0(%rax,%rax,1)
  651f20:	mov    0x68(%rsp),%rcx
  651f25:	mov    %rcx,%rsi
  651f28:	sub    $0x1,%rsi
  651f2c:	jb     65286d <valar_spiral_rs::poly::from_ntt+0xbfd>
  651f32:	mov    %rsi,%rax
  651f35:	mov    0x10(%rsp),%rsi
  651f3a:	shrx   %rcx,%rsi,%r9
  651f3f:	mov    %r9,%r8
  651f42:	add    %r9,%r8
  651f45:	je     652f15 <valar_spiral_rs::poly::from_ntt+0x12a5>
  651f4b:	mov    %rax,0x68(%rsp)
  651f50:	mov    %rsi,%rax
  651f53:	or     %r8,%rax
  651f56:	shr    $0x20,%rax
  651f5a:	je     651f70 <valar_spiral_rs::poly::from_ntt+0x300>
  651f5c:	mov    %rsi,%rax
  651f5f:	xor    %edx,%edx
  651f61:	div    %r8
  651f64:	jmp    651f77 <valar_spiral_rs::poly::from_ntt+0x307>
  651f66:	cs nopw 0x0(%rax,%rax,1)
  651f70:	mov    %esi,%eax
  651f72:	xor    %edx,%edx
  651f74:	div    %r8d
  651f77:	lea    -0x1(%rcx),%eax
  651f7a:	mov    $0x1,%ecx
  651f7f:	shlx   %rax,%rcx,%r15
  651f84:	sub    %rdx,%rsi
  651f87:	mov    %rsi,0x50(%rsp)
  651f8c:	test   %r9,%r9
  651f8f:	je     6521a0 <valar_spiral_rs::poly::from_ntt+0x530>
  651f95:	mov    %r9,%rax
  651f98:	shl    $0x4,%rax
  651f9c:	mov    %rax,0x78(%rsp)
  651fa1:	lea    0x0(,%r9,8),%rax
  651fa9:	mov    %rax,0xa8(%rsp)
  651fb1:	mov    $0x1,%eax
  651fb6:	mov    %rbx,0x20(%rsp)
  651fbb:	xor    %edx,%edx
  651fbd:	mov    %r8,0xc0(%rsp)
  651fc5:	mov    %r9,0xb8(%rsp)
  651fcd:	mov    %r15,0x80(%rsp)
  651fd5:	data16 cs nopw 0x0(%rax,%rax,1)
  651fe0:	mov    %r15,%rcx
  651fe3:	mov    %rdx,%r15
  651fe6:	add    %rcx,%r15
  651fe9:	cmp    %rdi,%r15
  651fec:	jae    653069 <valar_spiral_rs::poly::from_ntt+0x13f9>
  651ff2:	cmp    0x48(%rsp),%r15
  651ff7:	jae    653058 <valar_spiral_rs::poly::from_ntt+0x13e8>
  651ffd:	mov    %rax,0x88(%rsp)
  652005:	mov    0x50(%rsp),%rax
  65200a:	sub    %r8,%rax
  65200d:	jb     65304c <valar_spiral_rs::poly::from_ntt+0x13dc>
  652013:	mov    %rax,0x50(%rsp)
  652018:	mov    0xa0(%rsp),%rax
  652020:	mov    (%rax,%r15,8),%rax
  652024:	mov    %rax,0xd0(%rsp)
  65202c:	mov    0x70(%rsp),%rax
  652031:	mov    (%rax,%r15,8),%rax
  652035:	mov    %rax,0xc8(%rsp)
  65203d:	mov    0xa8(%rsp),%rax
  652045:	mov    0x20(%rsp),%rcx
  65204a:	add    %rcx,%rax
  65204d:	mov    %rax,0x58(%rsp)
  652052:	xor    %ebp,%ebp
  652054:	data16 data16 cs nopw 0x0(%rax,%rax,1)
  652060:	cmp    %rbp,%r8
  652063:	je     652ee6 <valar_spiral_rs::poly::from_ntt+0x1276>
  652069:	lea    (%r9,%rbp,1),%r15
  65206d:	cmp    %r8,%r15
  652070:	jae    652ef5 <valar_spiral_rs::poly::from_ntt+0x1285>
  652076:	mov    0x20(%rsp),%rax
  65207b:	mov    (%rax,%rbp,8),%r15
  65207f:	mov    0x58(%rsp),%rax
  652084:	mov    (%rax,%rbp,8),%r12
  652088:	mov    0x28(%rsp),%rbx
  65208d:	sub    %r12,%rbx
  652090:	add    %r15,%rbx
  652093:	mov    %rbx,%rdx
  652096:	mov    0xc8(%rsp),%rax
  65209e:	mulx   %rax,%rax,%rax
  6520a3:	mulx   0xd0(%rsp),%r13,%rcx
  6520ad:	mov    %rax,%rdx
  6520b0:	mov    0x30(%rsp),%rsi
  6520b5:	mulx   %rsi,%rdx,%rax
  6520ba:	sub    %rdx,%r13
  6520bd:	sbb    %rax,%rcx
  6520c0:	mov    %r13,%r14
  6520c3:	sub    %rsi,%r14
  6520c6:	sbb    $0x0,%rcx
  6520ca:	setb   %al
  6520cd:	movzbl %al,%edi
  6520d0:	vzeroupper
  6520d3:	call   64f360 <subtle::black_box>
  6520d8:	mov    0xb8(%rsp),%r9
  6520e0:	mov    0xc0(%rsp),%r8
  6520e8:	movzbl %al,%eax
  6520eb:	mov    %rax,%rcx
  6520ee:	neg    %rcx
  6520f1:	dec    %rax
  6520f4:	and    %r14,%rax
  6520f7:	and    %r13,%rcx
  6520fa:	or     %rax,%rcx
  6520fd:	add    %r15,%r12
  652100:	add    %r15,%r15
  652103:	cmp    %rbx,%r15
  652106:	mov    0x28(%rsp),%rax
  65210b:	mov    $0x0,%edx
  652110:	cmovb  %rdx,%rax
  652114:	sub    %rax,%r12
  652117:	test   $0x1,%bl
  65211a:	mov    $0x0,%eax
  65211f:	cmovne 0x30(%rsp),%rax
  652125:	add    %r12,%rax
  652128:	shr    $1,%rax
  65212b:	mov    0x20(%rsp),%rdx
  652130:	mov    %rax,(%rdx,%rbp,8)
  652134:	mov    0x58(%rsp),%rax
  652139:	mov    %rcx,(%rax,%rbp,8)
  65213d:	inc    %rbp
  652140:	cmp    %rbp,%r9
  652143:	jne    652060 <valar_spiral_rs::poly::from_ntt+0x3f0>
  652149:	mov    0x80(%rsp),%r15
  652151:	mov    0x88(%rsp),%rdx
  652159:	cmp    %r15,%rdx
  65215c:	mov    %rdx,%rax
  65215f:	adc    $0x0,%rax
  652163:	mov    0x20(%rsp),%rcx
  652168:	add    0x78(%rsp),%rcx
  65216d:	mov    %rcx,0x20(%rsp)
  652172:	cmp    %r15,%rdx
  652175:	mov    0x18(%rsp),%rbp
  65217a:	mov    0xb0(%rsp),%rbx
  652182:	mov    0x40(%rsp),%rdi
  652187:	jb     651fe0 <valar_spiral_rs::poly::from_ntt+0x370>
  65218d:	jmp    651f20 <valar_spiral_rs::poly::from_ntt+0x2b0>
  652192:	data16 data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  6521a0:	xor    %eax,%eax
  6521a2:	data16 data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  6521b0:	lea    (%r15,%rax,1),%rcx
  6521b4:	cmp    %rdi,%rcx
  6521b7:	jae    653089 <valar_spiral_rs::poly::from_ntt+0x1419>
  6521bd:	cmp    0x48(%rsp),%rcx
  6521c2:	jae    65309f <valar_spiral_rs::poly::from_ntt+0x142f>
  6521c8:	sub    %r8,0x50(%rsp)
  6521cd:	jb     65304c <valar_spiral_rs::poly::from_ntt+0x13dc>
  6521d3:	inc    %rax
  6521d6:	cmp    %r15,%rax
  6521d9:	jb     6521b0 <valar_spiral_rs::poly::from_ntt+0x540>
  6521db:	jmp    651f20 <valar_spiral_rs::poly::from_ntt+0x2b0>
  6521e0:	mov    0x8(%rsp),%rcx
  6521e5:	mov    0x30(%rcx),%rdi
  6521e9:	mov    0x8(%rcx),%rax
  6521ed:	mov    %rax,0x70(%rsp)
  6521f2:	mov    0x38(%rcx),%rdx
  6521f6:	mov    %rdx,0x68(%rsp)
  6521fb:	sub    $0x1,%rdx
  6521ff:	mov    $0x0,%eax
  652204:	cmovb  %rax,%rdx
  652208:	mov    %rdx,0x150(%rsp)
  652210:	mov    %rdi,%rdx
  652213:	shr    $0x2,%rdx
  652217:	mov    %edi,%eax
  652219:	and    $0x3,%eax
  65221c:	cmp    $0x1,%rax
  652220:	sbb    $0xffffffffffffffff,%rdx
  652224:	mov    %rdx,0x148(%rsp)
  65222c:	mov    0x10(%rcx),%rax
  652230:	mov    %rax,0xe0(%rsp)
  652238:	lea    0x0(,%rdi,8),%rax
  652240:	mov    %rax,0x158(%rsp)
  652248:	mov    $0x1,%eax
  65224d:	mov    0x100(%rsp),%rcx
  652255:	mov    %rcx,0x78(%rsp)
  65225a:	mov    %rbx,%r8
  65225d:	xor    %esi,%esi
  65225f:	mov    %rdi,0xa8(%rsp)
  652267:	jmp    6522a0 <valar_spiral_rs::poly::from_ntt+0x630>
  652269:	nopl   0x0(%rax)
  652270:	mov    0x10(%rsp),%rcx
  652275:	cmp    %rcx,%rsi
  652278:	mov    %rsi,%rax
  65227b:	adc    $0x0,%rax
  65227f:	mov    0x158(%rsp),%rdx
  652287:	add    %rdx,%r8
  65228a:	add    %rdx,0x78(%rsp)
  65228f:	cmp    %rcx,%rsi
  652292:	mov    0xb0(%rsp),%rbx
  65229a:	jae    652d90 <valar_spiral_rs::poly::from_ntt+0x1120>
  6522a0:	mov    %rax,%r9
  6522a3:	mov    %rdi,%rax
  6522a6:	imul   %rsi,%rax
  6522aa:	mov    %rax,%rcx
  6522ad:	add    %rdi,%rcx
  6522b0:	jb     652f21 <valar_spiral_rs::poly::from_ntt+0x12b1>
  6522b6:	mov    0x38(%rsp),%rdx
  6522bb:	cmp    %rdx,%rcx
  6522be:	ja     652f34 <valar_spiral_rs::poly::from_ntt+0x12c4>
  6522c4:	mov    %rsi,%r15
  6522c7:	cmp    0xe0(%rsp),%rsi
  6522cf:	jae    653111 <valar_spiral_rs::poly::from_ntt+0x14a1>
  6522d5:	lea    (%r15,%r15,2),%rax
  6522d9:	mov    0x70(%rsp),%rcx
  6522de:	mov    0x10(%rcx,%rax,8),%rsi
  6522e3:	cmp    $0x3,%rsi
  6522e7:	jb     65314b <valar_spiral_rs::poly::from_ntt+0x14db>
  6522ed:	mov    %r8,0x80(%rsp)
  6522f5:	je     6530fa <valar_spiral_rs::poly::from_ntt+0x148a>
  6522fb:	mov    %r9,0xa0(%rsp)
  652303:	cmp    $0x4,%r15
  652307:	jae    653125 <valar_spiral_rs::poly::from_ntt+0x14b5>
  65230d:	mov    0x8(%rsp),%rcx
  652312:	mov    0xa8(%rcx,%r15,8),%rcx
  65231a:	lea    (%rcx,%rcx,1),%rdx
  65231e:	mov    %rdx,0x58(%rsp)
  652323:	vmovq  %rdx,%xmm0
  652328:	mov    %rcx,0x20(%rsp)
  65232d:	vmovq  %rcx,%xmm1
  652332:	cmpq   $0x0,0x68(%rsp)
  652338:	je     652800 <valar_spiral_rs::poly::from_ntt+0xb90>
  65233e:	mov    0x70(%rsp),%rcx
  652343:	lea    (%rcx,%rax,8),%rax
  652347:	mov    0x8(%rax),%rax
  65234b:	mov    0x38(%rax),%rcx
  65234f:	mov    %rcx,0x30(%rsp)
  652354:	mov    0x40(%rax),%r8
  652358:	mov    0x50(%rax),%rcx
  65235c:	mov    %rcx,0x28(%rsp)
  652361:	mov    0x58(%rax),%rax
  652365:	mov    %rax,0x40(%rsp)
  65236a:	vpbroadcastq %xmm1,%ymm2
  65236f:	vpbroadcastq %xmm0,%ymm3
  652374:	mov    0x150(%rsp),%rax
  65237c:	mov    0x68(%rsp),%rcx
  652381:	mov    %r8,0xb8(%rsp)
  652389:	jmp    6523aa <valar_spiral_rs::poly::from_ntt+0x73a>
  65238b:	nopl   0x0(%rax,%rax,1)
  652390:	mov    0x48(%rsp),%rcx
  652395:	mov    %rcx,%rax
  652398:	sub    $0x1,%rax
  65239c:	mov    0xa8(%rsp),%rdi
  6523a4:	jb     652800 <valar_spiral_rs::poly::from_ntt+0xb90>
  6523aa:	shrx   %rcx,%rdi,%rbx
  6523af:	mov    %rbx,%r13
  6523b2:	add    %rbx,%r13
  6523b5:	je     652e4c <valar_spiral_rs::poly::from_ntt+0x11dc>
  6523bb:	mov    %rax,%rsi
  6523be:	mov    %rdi,%rax
  6523c1:	or     %r13,%rax
  6523c4:	shr    $0x20,%rax
  6523c8:	je     6523e0 <valar_spiral_rs::poly::from_ntt+0x770>
  6523ca:	mov    %rdi,%rax
  6523cd:	xor    %edx,%edx
  6523cf:	div    %r13
  6523d2:	jmp    6523e7 <valar_spiral_rs::poly::from_ntt+0x777>
  6523d4:	data16 data16 cs nopw 0x0(%rax,%rax,1)
  6523e0:	mov    %edi,%eax
  6523e2:	xor    %edx,%edx
  6523e4:	div    %r13d
  6523e7:	lea    -0x1(%rcx),%eax
  6523ea:	mov    $0x1,%ecx
  6523ef:	shlx   %rax,%rcx,%rax
  6523f4:	mov    %rdi,%rcx
  6523f7:	sub    %rdx,%rcx
  6523fa:	mov    %rbx,%r12
  6523fd:	shr    $0x2,%r12
  652401:	mov    %ebx,%edx
  652403:	and    $0x3,%edx
  652406:	cmp    $0x1,%rdx
  65240a:	sbb    $0xffffffffffffffff,%r12
  65240e:	cmp    $0x4,%rbx
  652412:	mov    %rsi,0x48(%rsp)
  652417:	jae    652660 <valar_spiral_rs::poly::from_ntt+0x9f0>
  65241d:	test   %rbx,%rbx
  652420:	je     6527b2 <valar_spiral_rs::poly::from_ntt+0xb42>
  652426:	lea    0x1(%rbx),%rdx
  65242a:	mov    %rdx,0x50(%rsp)
  65242f:	lea    0x2(%rbx),%rdx
  652433:	mov    %rdx,0x88(%rsp)
  65243b:	mov    0x28(%rsp),%rdx
  652440:	lea    (%rdx,%rax,8),%rdx
  652444:	mov    %rdx,0xd0(%rsp)
  65244c:	mov    0x30(%rsp),%rdx
  652451:	lea    (%rdx,%rax,8),%rdx
  652455:	mov    %rdx,0xc8(%rsp)
  65245d:	mov    %rbx,%rdx
  652460:	shl    $0x4,%rdx
  652464:	mov    %rdx,0xc0(%rsp)
  65246c:	mov    $0x1,%edi
  652471:	mov    0x78(%rsp),%r11
  652476:	xor    %edx,%edx
  652478:	jmp    6524a2 <valar_spiral_rs::poly::from_ntt+0x832>
  65247a:	nopw   0x0(%rax,%rax,1)
  652480:	lea    0x1(%rdx),%rdi
  652484:	add    0xc0(%rsp),%r11
  65248c:	cmp    %rax,%rdx
  65248f:	mov    0x18(%rsp),%rbp
  652494:	mov    0xb8(%rsp),%r8
  65249c:	jae    652390 <valar_spiral_rs::poly::from_ntt+0x720>
  6524a2:	lea    (%rax,%rdi,1),%r15
  6524a6:	dec    %r15
  6524a9:	cmp    %r8,%r15
  6524ac:	jae    652fbc <valar_spiral_rs::poly::from_ntt+0x134c>
  6524b2:	mov    %rdx,%r9
  6524b5:	mov    %rdi,%rdx
  6524b8:	mov    0x40(%rsp),%rdi
  6524bd:	cmp    %rdi,%r15
  6524c0:	jae    652f84 <valar_spiral_rs::poly::from_ntt+0x1314>
  6524c6:	sub    %r13,%rcx
  6524c9:	jb     652f01 <valar_spiral_rs::poly::from_ntt+0x1291>
  6524cf:	mov    0xc8(%rsp),%rsi
  6524d7:	mov    -0x8(%rsi,%rdx,8),%rdi
  6524dc:	mov    0xd0(%rsp),%rsi
  6524e4:	mov    -0x8(%rsi,%rdx,8),%r9
  6524e9:	mov    -0x10(%r11),%r15
  6524ed:	mov    -0x10(%r11,%rbx,8),%r12
  6524f2:	mov    0x58(%rsp),%rsi
  6524f7:	mov    %rsi,%r14
  6524fa:	sub    %r12,%r14
  6524fd:	add    %r15,%r14
  652500:	mov    %r14,%r8
  652503:	imul   %r9,%r8
  652507:	shr    $0x20,%r8
  65250b:	mov    %r14,%rbp
  65250e:	imul   %rdi,%rbp
  652512:	mov    0x20(%rsp),%r10
  652517:	imul   %r10,%r8
  65251b:	sub    %r8,%rbp
  65251e:	add    %r15,%r12
  652521:	add    %r15,%r15
  652524:	cmp    %r14,%r15
  652527:	mov    $0x0,%r15d
  65252d:	cmovb  %r15,%rsi
  652531:	sub    %rsi,%r12
  652534:	test   $0x1,%r14b
  652538:	mov    $0x0,%r8d
  65253e:	cmovne %r10,%r8
  652542:	add    %r12,%r8
  652545:	shr    $1,%r8
  652548:	mov    %r8,-0x10(%r11)
  65254c:	mov    %rbp,-0x10(%r11,%rbx,8)
  652551:	cmp    $0x1,%rbx
  652555:	je     652480 <valar_spiral_rs::poly::from_ntt+0x810>
  65255b:	cmp    %r13,0x50(%rsp)
  652560:	jae    652fcb <valar_spiral_rs::poly::from_ntt+0x135b>
  652566:	mov    -0x8(%r11),%r8
  65256a:	mov    -0x8(%r11,%rbx,8),%r14
  65256f:	mov    0x58(%rsp),%rsi
  652574:	mov    %rsi,%r15
  652577:	sub    %r14,%r15
  65257a:	add    %r8,%r15
  65257d:	mov    %r15,%r12
  652580:	imul   %r9,%r12
  652584:	shr    $0x20,%r12
  652588:	mov    %r15,%rbp
  65258b:	imul   %rdi,%rbp
  65258f:	mov    0x20(%rsp),%r10
  652594:	imul   %r10,%r12
  652598:	sub    %r12,%rbp
  65259b:	add    %r8,%r14
  65259e:	add    %r8,%r8
  6525a1:	cmp    %r15,%r8
  6525a4:	mov    %rsi,%r8
  6525a7:	mov    $0x0,%r12d
  6525ad:	cmovb  %r12,%r8
  6525b1:	sub    %r8,%r14
  6525b4:	test   $0x1,%r15b
  6525b8:	mov    $0x0,%r8d
  6525be:	cmovne %r10,%r8
  6525c2:	add    %r14,%r8
  6525c5:	shr    $1,%r8
  6525c8:	mov    %r8,-0x8(%r11)
  6525cc:	mov    %rbp,-0x8(%r11,%rbx,8)
  6525d1:	cmp    $0x2,%rbx
  6525d5:	je     652480 <valar_spiral_rs::poly::from_ntt+0x810>
  6525db:	cmp    %r13,0x88(%rsp)
  6525e3:	mov    0x18(%rsp),%rbp
  6525e8:	jae    652fed <valar_spiral_rs::poly::from_ntt+0x137d>
  6525ee:	mov    (%r11),%r8
  6525f1:	mov    (%r11,%rbx,8),%r14
  6525f5:	mov    0x58(%rsp),%rsi
  6525fa:	mov    %rsi,%r15
  6525fd:	sub    %r14,%r15
  652600:	add    %r8,%r15
  652603:	imul   %r15,%r9
  652607:	shr    $0x20,%r9
  65260b:	imul   %r15,%rdi
  65260f:	mov    0x20(%rsp),%r10
  652614:	imul   %r10,%r9
  652618:	sub    %r9,%rdi
  65261b:	add    %r8,%r14
  65261e:	add    %r8,%r8
  652621:	cmp    %r15,%r8
  652624:	mov    %rsi,%r8
  652627:	mov    $0x0,%r9d
  65262d:	cmovb  %r9,%r8
  652631:	sub    %r8,%r14
  652634:	test   $0x1,%r15b
  652638:	mov    $0x0,%r8d
  65263e:	cmovne %r10,%r8
  652642:	add    %r14,%r8
  652645:	shr    $1,%r8
  652648:	mov    %r8,(%r11)
  65264b:	mov    %rdi,(%r11,%rbx,8)
  65264f:	jmp    652480 <valar_spiral_rs::poly::from_ntt+0x810>
  652654:	data16 data16 cs nopw 0x0(%rax,%rax,1)
  652660:	mov    %rbx,%rsi
  652663:	shl    $0x4,%rsi
  652667:	lea    0x0(,%rbx,8),%r9
  65266f:	mov    $0x1,%edx
  652674:	mov    0x80(%rsp),%r10
  65267c:	xor    %r11d,%r11d
  65267f:	nop
  652680:	mov    %r11,%r15
  652683:	add    %rax,%r15
  652686:	cmp    %r8,%r15
  652689:	jae    652fbc <valar_spiral_rs::poly::from_ntt+0x134c>
  65268f:	cmp    0x40(%rsp),%r15
  652694:	jae    652f55 <valar_spiral_rs::poly::from_ntt+0x12e5>
  65269a:	sub    %r13,%rcx
  65269d:	jb     652f01 <valar_spiral_rs::poly::from_ntt+0x1291>
  6526a3:	mov    %rdx,%r11
  6526a6:	mov    0x28(%rsp),%rdx
  6526ab:	vpmovzxdq (%rdx,%r15,8),%xmm4
  6526b1:	vpbroadcastq %xmm4,%ymm4
  6526b6:	mov    0x30(%rsp),%rdx
  6526bb:	vmovq  (%rdx,%r15,8),%xmm5
  6526c1:	vpmovzxdq %xmm5,%xmm5
  6526c6:	vpbroadcastq %xmm5,%ymm5
  6526cb:	lea    (%r10,%r9,1),%rbp
  6526cf:	xor    %edi,%edi
  6526d1:	mov    %r12,%rdx
  6526d4:	data16 data16 cs nopw 0x0(%rax,%rax,1)
  6526e0:	cmp    %r13,%rdi
  6526e3:	jae    652e8b <valar_spiral_rs::poly::from_ntt+0x121b>
  6526e9:	lea    (%rbx,%rdi,1),%r15
  6526ed:	cmp    %r13,%r15
  6526f0:	jae    652e7f <valar_spiral_rs::poly::from_ntt+0x120f>
  6526f6:	dec    %rdx
  6526f9:	vmovdqa (%r10,%rdi,8),%ymm6
  6526ff:	vmovdqa 0x0(%rbp,%rdi,8),%ymm7
  652705:	vpsubq %ymm7,%ymm3,%ymm8
  652709:	vpaddq %ymm6,%ymm8,%ymm8
  65270d:	vpaddq %ymm6,%ymm6,%ymm9
  652711:	vpcmpgtq %ymm8,%ymm9,%ymm9
  652716:	vpand  %ymm3,%ymm9,%ymm9
  65271a:	vpaddq %ymm6,%ymm7,%ymm6
  65271e:	vpsubq %ymm9,%ymm6,%ymm6
  652723:	vpmuludq %ymm4,%ymm8,%ymm7
  652727:	vpsrlq $0x20,%ymm4,%ymm9
  65272c:	vpmuludq %ymm9,%ymm8,%ymm9
  652731:	vpsllq $0x20,%ymm9,%ymm9
  652737:	vpaddq %ymm7,%ymm9,%ymm7
  65273b:	vpsrlq $0x20,%ymm7,%ymm7
  652740:	vpsllq $0x3f,%ymm8,%ymm9
  652746:	vpcmpgtq %ymm9,%ymm11,%ymm9
  65274b:	vpand  %ymm2,%ymm9,%ymm9
  65274f:	vpaddq %ymm6,%ymm9,%ymm6
  652753:	vpsrlq $0x1,%ymm6,%ymm6
  652758:	vpmuludq %ymm5,%ymm8,%ymm9
  65275c:	vpsrlq $0x20,%ymm5,%ymm10
  652761:	vpmuludq %ymm10,%ymm8,%ymm8
  652766:	vpsllq $0x20,%ymm8,%ymm8
  65276c:	vpaddq %ymm8,%ymm9,%ymm8
  652771:	vpmuludq %ymm2,%ymm7,%ymm7
  652775:	vpsubq %ymm7,%ymm8,%ymm7
  652779:	vmovdqa %ymm6,(%r10,%rdi,8)
  65277f:	vmovdqa %ymm7,0x0(%rbp,%rdi,8)
  652785:	add    $0x4,%rdi
  652789:	test   %rdx,%rdx
  65278c:	jne    6526e0 <valar_spiral_rs::poly::from_ntt+0xa70>
  652792:	cmp    %rax,%r11
  652795:	mov    %r11,%rdx
  652798:	adc    $0x0,%rdx
  65279c:	add    %rsi,%r10
  65279f:	cmp    %rax,%r11
  6527a2:	mov    0x18(%rsp),%rbp
  6527a7:	jb     652680 <valar_spiral_rs::poly::from_ntt+0xa10>
  6527ad:	jmp    652390 <valar_spiral_rs::poly::from_ntt+0x720>
  6527b2:	xor    %edx,%edx
  6527b4:	data16 data16 cs nopw 0x0(%rax,%rax,1)
  6527c0:	lea    (%rax,%rdx,1),%rsi
  6527c4:	cmp    %r8,%rsi
  6527c7:	jae    652fb2 <valar_spiral_rs::poly::from_ntt+0x1342>
  6527cd:	mov    0x40(%rsp),%rdi
  6527d2:	cmp    %rdi,%rsi
  6527d5:	jae    652f99 <valar_spiral_rs::poly::from_ntt+0x1329>
  6527db:	sub    %r13,%rcx
  6527de:	jb     652f01 <valar_spiral_rs::poly::from_ntt+0x1291>
  6527e4:	inc    %rdx
  6527e7:	cmp    %rax,%rdx
  6527ea:	jb     6527c0 <valar_spiral_rs::poly::from_ntt+0xb50>
  6527ec:	jmp    652390 <valar_spiral_rs::poly::from_ntt+0x720>
  6527f1:	data16 data16 data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  652800:	test   %rdi,%rdi
  652803:	mov    0x80(%rsp),%r8
  65280b:	mov    0xa0(%rsp),%rsi
  652813:	je     652270 <valar_spiral_rs::poly::from_ntt+0x600>
  652819:	vpbroadcastq %xmm0,%ymm0
  65281e:	vpbroadcastq %xmm1,%ymm1
  652823:	xor    %r15d,%r15d
  652826:	mov    0x148(%rsp),%rax
  65282e:	xchg   %ax,%ax
  652830:	cmp    %rdi,%r15
  652833:	jae    65307a <valar_spiral_rs::poly::from_ntt+0x140a>
  652839:	vmovdqa (%r8,%r15,8),%ymm2
  65283f:	vpcmpgtq %ymm2,%ymm0,%ymm3
  652844:	vpandn %ymm0,%ymm3,%ymm3
  652848:	vpsubq %ymm3,%ymm2,%ymm2
  65284c:	vpcmpgtq %ymm2,%ymm1,%ymm3
  652851:	vpandn %ymm1,%ymm3,%ymm3
  652855:	vpsubq %ymm3,%ymm2,%ymm2
  652859:	vmovdqa %ymm2,(%r8,%r15,8)
  65285f:	add    $0x4,%r15
  652863:	dec    %rax
  652866:	jne    652830 <valar_spiral_rs::poly::from_ntt+0xbc0>
  652868:	jmp    652270 <valar_spiral_rs::poly::from_ntt+0x600>
  65286d:	mov    0x10(%rsp),%rsi
  652872:	test   %rsi,%rsi
  652875:	je     652d90 <valar_spiral_rs::poly::from_ntt+0x1120>
  65287b:	cmp    $0x3,%rsi
  65287f:	ja     6528da <valar_spiral_rs::poly::from_ntt+0xc6a>
  652881:	xor    %eax,%eax
  652883:	jmp    652dd5 <valar_spiral_rs::poly::from_ntt+0x1165>
  652888:	test   %r8,%r8
  65288b:	mov    0x38(%rsp),%rdx
  652890:	je     6528f3 <valar_spiral_rs::poly::from_ntt+0xc83>
  652892:	cmp    %rdx,%r8
  652895:	ja     652fdf <valar_spiral_rs::poly::from_ntt+0x136f>
  65289b:	test   %rdi,%rdi
  65289e:	je     653136 <valar_spiral_rs::poly::from_ntt+0x14c6>
  6528a4:	mov    0x10(%rax),%rsi
  6528a8:	cmp    $0x3,%rsi
  6528ac:	jb     65314b <valar_spiral_rs::poly::from_ntt+0x14db>
  6528b2:	mov    0x10(%rsp),%rdi
  6528b7:	je     6530fa <valar_spiral_rs::poly::from_ntt+0x148a>
  6528bd:	mov    0x8(%rsp),%rax
  6528c2:	mov    0xa8(%rax),%rax
  6528c9:	lea    (%rax,%rax,1),%rcx
  6528cd:	cmp    $0x4,%rdi
  6528d1:	jae    652915 <valar_spiral_rs::poly::from_ntt+0xca5>
  6528d3:	xor    %edx,%edx
  6528d5:	jmp    652c35 <valar_spiral_rs::poly::from_ntt+0xfc5>
  6528da:	vmovq  0x28(%rsp),%xmm0
  6528e0:	vmovq  0x30(%rsp),%xmm1
  6528e6:	cmp    $0x10,%rsi
  6528ea:	jae    652930 <valar_spiral_rs::poly::from_ntt+0xcc0>
  6528ec:	xor    %eax,%eax
  6528ee:	jmp    652a36 <valar_spiral_rs::poly::from_ntt+0xdc6>
  6528f3:	test   %rdi,%rdi
  6528f6:	je     653136 <valar_spiral_rs::poly::from_ntt+0x14c6>
  6528fc:	mov    0x10(%rax),%rsi
  652900:	cmp    $0x3,%rsi
  652904:	jb     65314b <valar_spiral_rs::poly::from_ntt+0x14db>
  65290a:	jne    652d90 <valar_spiral_rs::poly::from_ntt+0x1120>
  652910:	jmp    6530fa <valar_spiral_rs::poly::from_ntt+0x148a>
  652915:	vmovq  %rax,%xmm0
  65291a:	vmovq  %rcx,%xmm1
  65291f:	cmp    $0x10,%rdi
  652923:	jae    652aa2 <valar_spiral_rs::poly::from_ntt+0xe32>
  652929:	xor    %edx,%edx
  65292b:	jmp    652bb2 <valar_spiral_rs::poly::from_ntt+0xf42>
  652930:	mov    %rsi,%rax
  652933:	and    $0xfffffffffffffff0,%rax
  652937:	vpbroadcastq %xmm0,%ymm2
  65293c:	vpbroadcastq %xmm1,%ymm3
  652941:	xor    %ecx,%ecx
  652943:	vmovdqu 0x160(%rsp),%ymm13
  65294c:	nopl   0x0(%rax)
  652950:	vmovdqu (%rbx,%rcx,8),%ymm4
  652955:	vmovdqu 0x20(%rbx,%rcx,8),%ymm5
  65295b:	vmovdqu 0x40(%rbx,%rcx,8),%ymm6
  652961:	vmovdqu 0x60(%rbx,%rcx,8),%ymm7
  652967:	vpxor  %ymm2,%ymm13,%ymm8
  65296b:	vpxor  %ymm4,%ymm13,%ymm9
  65296f:	vpcmpgtq %ymm9,%ymm8,%ymm9
  652974:	vpandn %ymm2,%ymm9,%ymm9
  652978:	vpxor  %ymm5,%ymm13,%ymm10
  65297c:	vpcmpgtq %ymm10,%ymm8,%ymm10
  652981:	vpandn %ymm2,%ymm10,%ymm10
  652985:	vpxor  %ymm6,%ymm13,%ymm11
  652989:	vpcmpgtq %ymm11,%ymm8,%ymm11
  65298e:	vpandn %ymm2,%ymm11,%ymm11
  652992:	vpxor  %ymm7,%ymm13,%ymm12
  652996:	vpcmpgtq %ymm12,%ymm8,%ymm8
  65299b:	vpandn %ymm2,%ymm8,%ymm8
  65299f:	vpsubq %ymm9,%ymm4,%ymm4
  6529a4:	vpsubq %ymm10,%ymm5,%ymm5
  6529a9:	vpsubq %ymm11,%ymm6,%ymm6
  6529ae:	vpsubq %ymm8,%ymm7,%ymm7
  6529b3:	vpxor  %ymm4,%ymm13,%ymm8
  6529b7:	vpxor  %ymm3,%ymm13,%ymm9
  6529bb:	vpcmpgtq %ymm8,%ymm9,%ymm8
  6529c0:	vpandn %ymm3,%ymm8,%ymm8
  6529c4:	vpxor  %ymm5,%ymm13,%ymm10
  6529c8:	vpcmpgtq %ymm10,%ymm9,%ymm10
  6529cd:	vpandn %ymm3,%ymm10,%ymm10
  6529d1:	vpxor  %ymm6,%ymm13,%ymm11
  6529d5:	vpcmpgtq %ymm11,%ymm9,%ymm11
  6529da:	vpandn %ymm3,%ymm11,%ymm11
  6529de:	vpxor  %ymm7,%ymm13,%ymm12
  6529e2:	vpcmpgtq %ymm12,%ymm9,%ymm9
  6529e7:	vpandn %ymm3,%ymm9,%ymm9
  6529eb:	vpsubq %ymm8,%ymm4,%ymm4
  6529f0:	vpsubq %ymm10,%ymm5,%ymm5
  6529f5:	vpsubq %ymm11,%ymm6,%ymm6
  6529fa:	vpsubq %ymm9,%ymm7,%ymm7
  6529ff:	vmovdqu %ymm4,(%rbx,%rcx,8)
  652a04:	vmovdqu %ymm5,0x20(%rbx,%rcx,8)
  652a0a:	vmovdqu %ymm6,0x40(%rbx,%rcx,8)
  652a10:	vmovdqu %ymm7,0x60(%rbx,%rcx,8)
  652a16:	add    $0x10,%rcx
  652a1a:	cmp    %rcx,%rax
  652a1d:	jne    652950 <valar_spiral_rs::poly::from_ntt+0xce0>
  652a23:	cmp    %rax,%rsi
  652a26:	je     652d90 <valar_spiral_rs::poly::from_ntt+0x1120>
  652a2c:	test   $0xc,%sil
  652a30:	je     652dd5 <valar_spiral_rs::poly::from_ntt+0x1165>
  652a36:	mov    %rax,%rcx
  652a39:	mov    %rsi,%rax
  652a3c:	and    $0xfffffffffffffffc,%rax
  652a40:	vpbroadcastq %xmm0,%ymm0
  652a45:	vpbroadcastq %xmm1,%ymm1
  652a4a:	vmovdqu 0x160(%rsp),%ymm5
  652a53:	data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  652a60:	vmovdqu (%rbx,%rcx,8),%ymm2
  652a65:	vpxor  %ymm5,%ymm0,%ymm3
  652a69:	vpxor  %ymm5,%ymm2,%ymm4
  652a6d:	vpcmpgtq %ymm4,%ymm3,%ymm3
  652a72:	vpandn %ymm0,%ymm3,%ymm3
  652a76:	vpsubq %ymm3,%ymm2,%ymm2
  652a7a:	vpxor  %ymm5,%ymm2,%ymm3
  652a7e:	vpxor  %ymm5,%ymm1,%ymm4
  652a82:	vpcmpgtq %ymm3,%ymm4,%ymm3
  652a87:	vpandn %ymm1,%ymm3,%ymm3
  652a8b:	vpsubq %ymm3,%ymm2,%ymm2
  652a8f:	vmovdqu %ymm2,(%rbx,%rcx,8)
  652a94:	add    $0x4,%rcx
  652a98:	cmp    %rcx,%rax
  652a9b:	jne    652a60 <valar_spiral_rs::poly::from_ntt+0xdf0>
  652a9d:	jmp    652dd0 <valar_spiral_rs::poly::from_ntt+0x1160>
  652aa2:	mov    %rdi,%rdx
  652aa5:	and    $0xfffffffffffffff0,%rdx
  652aa9:	vpbroadcastq %xmm0,%ymm2
  652aae:	vpbroadcastq %xmm1,%ymm3
  652ab3:	vmovdqu 0x160(%rsp),%ymm14
  652abc:	vpxor  %ymm3,%ymm14,%ymm4
  652ac0:	vpxor  %ymm2,%ymm14,%ymm5
  652ac4:	xor    %esi,%esi
  652ac6:	cs nopw 0x0(%rax,%rax,1)
  652ad0:	vmovdqu (%rbx,%rsi,8),%ymm6
  652ad5:	vmovdqu 0x20(%rbx,%rsi,8),%ymm7
  652adb:	vmovdqu 0x40(%rbx,%rsi,8),%ymm8
  652ae1:	vmovdqu 0x60(%rbx,%rsi,8),%ymm9
  652ae7:	vpxor  %ymm6,%ymm14,%ymm10
  652aeb:	vpcmpgtq %ymm10,%ymm4,%ymm10
  652af0:	vpandn %ymm3,%ymm10,%ymm10
  652af4:	vpxor  %ymm7,%ymm14,%ymm11
  652af8:	vpcmpgtq %ymm11,%ymm4,%ymm11
  652afd:	vpandn %ymm3,%ymm11,%ymm11
  652b01:	vpxor  %ymm14,%ymm8,%ymm12
  652b06:	vpcmpgtq %ymm12,%ymm4,%ymm12
  652b0b:	vpandn %ymm3,%ymm12,%ymm12
  652b0f:	vpxor  %ymm14,%ymm9,%ymm13
  652b14:	vpcmpgtq %ymm13,%ymm4,%ymm13
  652b19:	vpandn %ymm3,%ymm13,%ymm13
  652b1d:	vpsubq %ymm10,%ymm6,%ymm6
  652b22:	vpsubq %ymm11,%ymm7,%ymm7
  652b27:	vpsubq %ymm12,%ymm8,%ymm8
  652b2c:	vpsubq %ymm13,%ymm9,%ymm9
  652b31:	vpxor  %ymm6,%ymm14,%ymm10
  652b35:	vpcmpgtq %ymm10,%ymm5,%ymm10
  652b3a:	vpandn %ymm2,%ymm10,%ymm10
  652b3e:	vpxor  %ymm7,%ymm14,%ymm11
  652b42:	vpcmpgtq %ymm11,%ymm5,%ymm11
  652b47:	vpandn %ymm2,%ymm11,%ymm11
  652b4b:	vpxor  %ymm14,%ymm8,%ymm12
  652b50:	vpcmpgtq %ymm12,%ymm5,%ymm12
  652b55:	vpandn %ymm2,%ymm12,%ymm12
  652b59:	vpxor  %ymm14,%ymm9,%ymm13
  652b5e:	vpcmpgtq %ymm13,%ymm5,%ymm13
  652b63:	vpandn %ymm2,%ymm13,%ymm13
  652b67:	vpsubq %ymm10,%ymm6,%ymm6
  652b6c:	vpsubq %ymm11,%ymm7,%ymm7
  652b71:	vpsubq %ymm12,%ymm8,%ymm8
  652b76:	vpsubq %ymm13,%ymm9,%ymm9
  652b7b:	vmovdqu %ymm6,(%rbx,%rsi,8)
  652b80:	vmovdqu %ymm7,0x20(%rbx,%rsi,8)
  652b86:	vmovdqu %ymm8,0x40(%rbx,%rsi,8)
  652b8c:	vmovdqu %ymm9,0x60(%rbx,%rsi,8)
  652b92:	add    $0x10,%rsi
  652b96:	cmp    %rsi,%rdx
  652b99:	jne    652ad0 <valar_spiral_rs::poly::from_ntt+0xe60>
  652b9f:	cmp    %rdx,%rdi
  652ba2:	je     652d90 <valar_spiral_rs::poly::from_ntt+0x1120>
  652ba8:	test   $0xc,%dil
  652bac:	je     652c35 <valar_spiral_rs::poly::from_ntt+0xfc5>
  652bb2:	mov    %rdx,%rsi
  652bb5:	mov    %rdi,%rdx
  652bb8:	and    $0xfffffffffffffffc,%rdx
  652bbc:	vpbroadcastq %xmm0,%ymm0
  652bc1:	vpbroadcastq %xmm1,%ymm1
  652bc6:	vmovdqu 0x160(%rsp),%ymm6
  652bcf:	vpxor  %ymm6,%ymm1,%ymm2
  652bd3:	vpxor  %ymm6,%ymm0,%ymm3
  652bd7:	nopw   0x0(%rax,%rax,1)
  652be0:	vmovdqu (%rbx,%rsi,8),%ymm4
  652be5:	vpxor  %ymm6,%ymm4,%ymm5
  652be9:	vpcmpgtq %ymm5,%ymm2,%ymm5
  652bee:	vpandn %ymm1,%ymm5,%ymm5
  652bf2:	vpsubq %ymm5,%ymm4,%ymm4
  652bf6:	vpxor  %ymm6,%ymm4,%ymm5
  652bfa:	vpcmpgtq %ymm5,%ymm3,%ymm5
  652bff:	vpandn %ymm0,%ymm5,%ymm5
  652c03:	vpsubq %ymm5,%ymm4,%ymm4
  652c07:	vmovdqu %ymm4,(%rbx,%rsi,8)
  652c0c:	add    $0x4,%rsi
  652c10:	cmp    %rsi,%rdx
  652c13:	jne    652be0 <valar_spiral_rs::poly::from_ntt+0xf70>
  652c15:	cmp    %rdx,%rdi
  652c18:	jne    652c35 <valar_spiral_rs::poly::from_ntt+0xfc5>
  652c1a:	jmp    652d90 <valar_spiral_rs::poly::from_ntt+0x1120>
  652c1f:	nop
  652c20:	sub    %rdi,%rsi
  652c23:	mov    %rsi,(%rbx,%rdx,8)
  652c27:	inc    %rdx
  652c2a:	cmp    %rdx,0x10(%rsp)
  652c2f:	je     652d90 <valar_spiral_rs::poly::from_ntt+0x1120>
  652c35:	mov    (%rbx,%rdx,8),%rsi
  652c39:	mov    $0x0,%edi
  652c3e:	cmp    %rcx,%rsi
  652c41:	jb     652c46 <valar_spiral_rs::poly::from_ntt+0xfd6>
  652c43:	mov    %rcx,%rdi
  652c46:	sub    %rdi,%rsi
  652c49:	mov    $0x0,%edi
  652c4e:	cmp    %rax,%rsi
  652c51:	jb     652c20 <valar_spiral_rs::poly::from_ntt+0xfb0>
  652c53:	mov    %rax,%rdi
  652c56:	jmp    652c20 <valar_spiral_rs::poly::from_ntt+0xfb0>
  652c58:	nopl   0x0(%rax,%rax,1)
  652c60:	mov    0x90(%rsp),%rax
  652c68:	mov    (%rax),%rax
  652c6b:	cmp    $0x1,%rax
  652c6f:	jne    652c90 <valar_spiral_rs::poly::from_ntt+0x1020>
  652c71:	cmp    %rcx,%r9
  652c74:	jae    6530d0 <valar_spiral_rs::poly::from_ntt+0x1460>
  652c7a:	mov    (%rbx,%r9,8),%rax
  652c7e:	jmp    652d57 <valar_spiral_rs::poly::from_ntt+0x10e7>
  652c83:	data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  652c90:	cmp    %rcx,%r9
  652c93:	jae    6530cb <valar_spiral_rs::poly::from_ntt+0x145b>
  652c99:	mov    0x8(%rsp),%rdx
  652c9e:	mov    0x30(%rdx),%rdi
  652ca2:	add    %r9,%rdi
  652ca5:	cmp    %rcx,%rdi
  652ca8:	jae    6530dc <valar_spiral_rs::poly::from_ntt+0x146c>
  652cae:	cmp    $0x2,%rax
  652cb2:	jne    652ead <valar_spiral_rs::poly::from_ntt+0x123d>
  652cb8:	mov    (%rbx,%r9,8),%rdx
  652cbc:	mov    (%rbx,%rdi,8),%rax
  652cc0:	mov    0x8(%rsp),%r11
  652cc5:	mulx   0xa0(%r11),%r10,%rdi
  652cce:	mov    %rax,%rdx
  652cd1:	mulx   0x98(%r11),%rax,%rcx
  652cda:	add    %r10,%rax
  652cdd:	adc    %rdi,%rcx
  652ce0:	mov    0x88(%r11),%r14
  652ce7:	mov    0x90(%r11),%rdi
  652cee:	mov    %rax,%rdx
  652cf1:	mulx   %r14,%r11,%r11
  652cf6:	mulx   %rdi,%rbx,%r10
  652cfb:	mov    %rcx,%rdx
  652cfe:	mulx   %r14,%r15,%r14
  652d03:	mov    $0x0,%edx
  652d08:	add    %rbx,%r11
  652d0b:	jb     652d18 <valar_spiral_rs::poly::from_ntt+0x10a8>
  652d0d:	mov    %r11,%r12
  652d10:	not    %r12
  652d13:	cmp    %r15,%r12
  652d16:	jb     652d7a <valar_spiral_rs::poly::from_ntt+0x110a>
  652d18:	mov    0x8(%rsp),%r15
  652d1d:	mov    0xc8(%r15),%r15
  652d24:	imul   %rcx,%rdi
  652d28:	add    %r14,%rdi
  652d2b:	cmp    %rbx,%r11
  652d2e:	adc    %r10,%rdi
  652d31:	add    %rdx,%rdi
  652d34:	imul   %r15,%rdi
  652d38:	sub    %rdi,%rax
  652d3b:	cmp    %r15,%rax
  652d3e:	mov    $0x0,%ecx
  652d43:	cmovae %r15,%rcx
  652d47:	sub    %rcx,%rax
  652d4a:	mov    0x38(%rsp),%rcx
  652d4f:	mov    0xb0(%rsp),%rbx
  652d57:	cmp    %r9,0x98(%rsp)
  652d5f:	je     6530b7 <valar_spiral_rs::poly::from_ntt+0x1447>
  652d65:	mov    %rax,(%r8,%r9,8)
  652d69:	inc    %r9
  652d6c:	cmp    %r9,%rsi
  652d6f:	jne    652c60 <valar_spiral_rs::poly::from_ntt+0xff0>
  652d75:	jmp    651db0 <valar_spiral_rs::poly::from_ntt+0x140>
  652d7a:	mov    $0x1,%edx
  652d7f:	jmp    652d18 <valar_spiral_rs::poly::from_ntt+0x10a8>
  652d81:	data16 data16 data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  652d90:	mov    0x8(%rsp),%rax
  652d95:	mov    0x30(%rax),%rsi
  652d99:	test   %rsi,%rsi
  652d9c:	mov    0x38(%rsp),%rcx
  652da1:	je     651db0 <valar_spiral_rs::poly::from_ntt+0x140>
  652da7:	mov    0x98(%rsp),%r8
  652daf:	imul   0xd8(%rsp),%r8
  652db8:	add    0x108(%rsp),%r8
  652dc0:	xor    %r9d,%r9d
  652dc3:	jmp    652c60 <valar_spiral_rs::poly::from_ntt+0xff0>
  652dc8:	nopl   0x0(%rax,%rax,1)
  652dd0:	cmp    %rax,%rsi
  652dd3:	je     652d90 <valar_spiral_rs::poly::from_ntt+0x1120>
  652dd5:	mov    (%rbx,%rax,8),%rcx
  652dd9:	mov    $0x0,%edx
  652dde:	cmp    0x28(%rsp),%rcx
  652de3:	jb     652dea <valar_spiral_rs::poly::from_ntt+0x117a>
  652de5:	mov    0x28(%rsp),%rdx
  652dea:	sub    %rdx,%rcx
  652ded:	mov    $0x0,%edx
  652df2:	cmp    0x30(%rsp),%rcx
  652df7:	jb     652dfe <valar_spiral_rs::poly::from_ntt+0x118e>
  652df9:	mov    0x30(%rsp),%rdx
  652dfe:	sub    %rdx,%rcx
  652e01:	mov    %rcx,(%rbx,%rax,8)
  652e05:	inc    %rax
  652e08:	jmp    652dd0 <valar_spiral_rs::poly::from_ntt+0x1160>
  652e0a:	mov    0xf8(%rsp),%rax
  652e12:	add    0xe8(%rsp),%rax
  652e1a:	cmp    0xf0(%rsp),%rdi
  652e22:	jne    651d7f <valar_spiral_rs::poly::from_ntt+0x10f>
  652e28:	mov    0x0(%rbp),%rax
  652e2c:	inc    %rax
  652e2f:	jmp    652e33 <valar_spiral_rs::poly::from_ntt+0x11c3>
  652e31:	xor    %eax,%eax
  652e33:	mov    %rax,0x0(%rbp)
  652e37:	add    $0x1b8,%rsp
  652e3e:	pop    %rbx
  652e3f:	pop    %r12
  652e41:	pop    %r13
  652e43:	pop    %r14
  652e45:	pop    %r15
  652e47:	pop    %rbp
  652e48:	vzeroupper
  652e4b:	ret
  652e4c:	lea    0x829f5(%rip),%rsi        # 6d5848 <tokio::runtime::task::waker::WAKER_VTABLE+0x2570>
  652e53:	lea    0x188(%rsp),%rdi
  652e5b:	lea    0x82e06(%rip),%rax        # 6d5c68 <tokio::runtime::task::waker::WAKER_VTABLE+0x2990>
  652e62:	mov    %rax,(%rdi)
  652e65:	vmovdqa -0x5d752d(%rip),%ymm0        # 7b940 <GCC_except_table6769+0x36f0>
  652e6d:	vmovdqu %ymm0,0x8(%rdi)
  652e72:	vzeroupper
  652e75:	call   2883e0 <core::panicking::panic_fmt>
  652e7a:	jmp    6530f8 <valar_spiral_rs::poly::from_ntt+0x1488>
  652e7f:	lea    0x82a3a(%rip),%rdx        # 6d58c0 <tokio::runtime::task::waker::WAKER_VTABLE+0x25e8>
  652e86:	mov    %r13,%rsi
  652e89:	jmp    652e98 <valar_spiral_rs::poly::from_ntt+0x1228>
  652e8b:	mov    %rdi,%r15
  652e8e:	mov    %r13,%rsi
  652e91:	lea    0x82a10(%rip),%rdx        # 6d58a8 <tokio::runtime::task::waker::WAKER_VTABLE+0x25d0>
  652e98:	mov    0x18(%rsp),%rbp
  652e9d:	mov    %r15,%rdi
  652ea0:	vzeroupper
  652ea3:	call   28be70 <core::panicking::panic_bounds_check>
  652ea8:	jmp    6530f8 <valar_spiral_rs::poly::from_ntt+0x1488>
  652ead:	movq   $0x0,0x188(%rsp)
  652eb9:	lea    -0x5d69e0(%rip),%rdx        # 7c4e0 <GCC_except_table6769+0x4290>
  652ec0:	lea    0x82e11(%rip),%r8        # 6d5cd8 <tokio::runtime::task::waker::WAKER_VTABLE+0x2a00>
  652ec7:	lea    0x188(%rsp),%rcx
  652ecf:	xor    %edi,%edi
  652ed1:	mov    0x90(%rsp),%rsi
  652ed9:	vzeroupper
  652edc:	call   290b1a <core::panicking::assert_failed>
  652ee1:	jmp    6530f8 <valar_spiral_rs::poly::from_ntt+0x1488>
  652ee6:	mov    %rbp,%r15
  652ee9:	mov    %r8,%rsi
  652eec:	lea    0x828ad(%rip),%rdx        # 6d57a0 <tokio::runtime::task::waker::WAKER_VTABLE+0x24c8>
  652ef3:	jmp    652e98 <valar_spiral_rs::poly::from_ntt+0x1228>
  652ef5:	lea    0x828bc(%rip),%rdx        # 6d57b8 <tokio::runtime::task::waker::WAKER_VTABLE+0x24e0>
  652efc:	mov    %r8,%rsi
  652eff:	jmp    652e98 <valar_spiral_rs::poly::from_ntt+0x1228>
  652f01:	lea    0x82988(%rip),%rdi        # 6d5890 <tokio::runtime::task::waker::WAKER_VTABLE+0x25b8>
  652f08:	vzeroupper
  652f0b:	call   288400 <core::option::unwrap_failed>
  652f10:	jmp    6530f8 <valar_spiral_rs::poly::from_ntt+0x1488>
  652f15:	lea    0x82824(%rip),%rsi        # 6d5740 <tokio::runtime::task::waker::WAKER_VTABLE+0x2468>
  652f1c:	jmp    652e53 <valar_spiral_rs::poly::from_ntt+0x11e3>
  652f21:	mov    %rcx,0x10(%rsp)
  652f26:	lea    0x829c3(%rip),%rcx        # 6d58f0 <tokio::runtime::task::waker::WAKER_VTABLE+0x2618>
  652f2d:	mov    0x38(%rsp),%rdx
  652f32:	jmp    652f40 <valar_spiral_rs::poly::from_ntt+0x12d0>
  652f34:	mov    %rcx,0x10(%rsp)
  652f39:	lea    0x829b0(%rip),%rcx        # 6d58f0 <tokio::runtime::task::waker::WAKER_VTABLE+0x2618>
  652f40:	mov    %rax,%rdi
  652f43:	mov    0x10(%rsp),%rsi
  652f48:	vzeroupper
  652f4b:	call   2896c0 <core::slice::index::slice_index_fail>
  652f50:	jmp    6530f8 <valar_spiral_rs::poly::from_ntt+0x1488>
  652f55:	lea    0x8291c(%rip),%rdx        # 6d5878 <tokio::runtime::task::waker::WAKER_VTABLE+0x25a0>
  652f5c:	mov    0x40(%rsp),%rsi
  652f61:	jmp    652e9d <valar_spiral_rs::poly::from_ntt+0x122d>
  652f66:	lea    0x82bf3(%rip),%rcx        # 6d5b60 <tokio::runtime::task::waker::WAKER_VTABLE+0x2888>
  652f6d:	xor    %edi,%edi
  652f6f:	mov    %rdx,%rsi
  652f72:	mov    0x38(%rsp),%rdx
  652f77:	vzeroupper
  652f7a:	call   2896c0 <core::slice::index::slice_index_fail>
  652f7f:	jmp    6530f8 <valar_spiral_rs::poly::from_ntt+0x1488>
  652f84:	add    %rax,%r9
  652f87:	mov    %rdi,%rsi
  652f8a:	mov    %r9,%r15
  652f8d:	lea    0x828e4(%rip),%rdx        # 6d5878 <tokio::runtime::task::waker::WAKER_VTABLE+0x25a0>
  652f94:	jmp    652e9d <valar_spiral_rs::poly::from_ntt+0x122d>
  652f99:	cmp    %rax,%rdi
  652f9c:	cmova  %rdi,%rax
  652fa0:	mov    %rdi,%rsi
  652fa3:	mov    %rax,%r15
  652fa6:	lea    0x828cb(%rip),%rdx        # 6d5878 <tokio::runtime::task::waker::WAKER_VTABLE+0x25a0>
  652fad:	jmp    652e9d <valar_spiral_rs::poly::from_ntt+0x122d>
  652fb2:	cmp    %rax,%r8
  652fb5:	cmova  %r8,%rax
  652fb9:	mov    %rax,%r15
  652fbc:	mov    %r8,%rsi
  652fbf:	lea    0x8289a(%rip),%rdx        # 6d5860 <tokio::runtime::task::waker::WAKER_VTABLE+0x2588>
  652fc6:	jmp    652e9d <valar_spiral_rs::poly::from_ntt+0x122d>
  652fcb:	mov    0x50(%rsp),%r15
  652fd0:	mov    %r13,%rsi
  652fd3:	lea    0x828fe(%rip),%rdx        # 6d58d8 <tokio::runtime::task::waker::WAKER_VTABLE+0x2600>
  652fda:	jmp    652e98 <valar_spiral_rs::poly::from_ntt+0x1228>
  652fdf:	xor    %eax,%eax
  652fe1:	lea    0x827e8(%rip),%rcx        # 6d57d0 <tokio::runtime::task::waker::WAKER_VTABLE+0x24f8>
  652fe8:	jmp    652f40 <valar_spiral_rs::poly::from_ntt+0x12d0>
  652fed:	mov    0x88(%rsp),%r15
  652ff5:	mov    %r13,%rsi
  652ff8:	lea    0x828d9(%rip),%rdx        # 6d58d8 <tokio::runtime::task::waker::WAKER_VTABLE+0x2600>
  652fff:	jmp    652e9d <valar_spiral_rs::poly::from_ntt+0x122d>
  653004:	mov    %fs:0x0,%rax
  65300d:	lea    -0x80(%rax),%rax
  653014:	mov    %rdi,%rbx
  653017:	mov    %rax,%rdi
  65301a:	mov    %rsi,%r14
  65301d:	call   653180 <std::sys::thread_local::native::lazy::Storage<T,D>::get_or_init_slow>
  653022:	mov    %r14,%rsi
  653025:	mov    %rbx,%rdi
  653028:	mov    %rax,%rbp
  65302b:	test   %rax,%rax
  65302e:	jne    651ca9 <valar_spiral_rs::poly::from_ntt+0x39>
  653034:	lea    0x82d5d(%rip),%rdi        # 6d5d98 <tokio::runtime::task::waker::WAKER_VTABLE+0x2ac0>
  65303b:	call   4b93c0 <std::thread::local::panic_access_error>
  653040:	lea    0x82b31(%rip),%rdi        # 6d5b78 <tokio::runtime::task::waker::WAKER_VTABLE+0x28a0>
  653047:	call   293100 <core::cell::panic_already_borrowed>
  65304c:	lea    0x82735(%rip),%rdi        # 6d5788 <tokio::runtime::task::waker::WAKER_VTABLE+0x24b0>
  653053:	jmp    652f08 <valar_spiral_rs::poly::from_ntt+0x1298>
  653058:	mov    0x48(%rsp),%rsi
  65305d:	lea    0x8270c(%rip),%rdx        # 6d5770 <tokio::runtime::task::waker::WAKER_VTABLE+0x2498>
  653064:	jmp    652e9d <valar_spiral_rs::poly::from_ntt+0x122d>
  653069:	lea    0x826e8(%rip),%rdx        # 6d5758 <tokio::runtime::task::waker::WAKER_VTABLE+0x2480>
  653070:	mov    0x40(%rsp),%rsi
  653075:	jmp    652e9d <valar_spiral_rs::poly::from_ntt+0x122d>
  65307a:	mov    %rdi,%rsi
  65307d:	lea    0x827ac(%rip),%rdx        # 6d5830 <tokio::runtime::task::waker::WAKER_VTABLE+0x2558>
  653084:	jmp    652e9d <valar_spiral_rs::poly::from_ntt+0x122d>
  653089:	cmp    %r15,%rdi
  65308c:	cmova  %rdi,%r15
  653090:	mov    %rdi,%rsi
  653093:	lea    0x826be(%rip),%rdx        # 6d5758 <tokio::runtime::task::waker::WAKER_VTABLE+0x2480>
  65309a:	jmp    652e9d <valar_spiral_rs::poly::from_ntt+0x122d>
  65309f:	mov    0x48(%rsp),%rsi
  6530a4:	cmp    %r15,%rsi
  6530a7:	cmova  %rsi,%r15
  6530ab:	lea    0x826be(%rip),%rdx        # 6d5770 <tokio::runtime::task::waker::WAKER_VTABLE+0x2498>
  6530b2:	jmp    652e9d <valar_spiral_rs::poly::from_ntt+0x122d>
  6530b7:	mov    0x98(%rsp),%rcx
  6530bf:	mov    %rcx,%rdi
  6530c2:	lea    0x82a7f(%rip),%rax        # 6d5b48 <tokio::runtime::task::waker::WAKER_VTABLE+0x2870>
  6530c9:	jmp    6530e3 <valar_spiral_rs::poly::from_ntt+0x1473>
  6530cb:	mov    %r9,%rdi
  6530ce:	jmp    6530e8 <valar_spiral_rs::poly::from_ntt+0x1478>
  6530d0:	mov    %r9,%rdi
  6530d3:	lea    0x82bb6(%rip),%rax        # 6d5c90 <tokio::runtime::task::waker::WAKER_VTABLE+0x29b8>
  6530da:	jmp    6530e3 <valar_spiral_rs::poly::from_ntt+0x1473>
  6530dc:	lea    0x82bdd(%rip),%rax        # 6d5cc0 <tokio::runtime::task::waker::WAKER_VTABLE+0x29e8>
  6530e3:	mov    %rax,0x60(%rsp)
  6530e8:	mov    %rcx,%rsi
  6530eb:	mov    0x60(%rsp),%rdx
  6530f0:	vzeroupper
  6530f3:	call   28be70 <core::panicking::panic_bounds_check>
  6530f8:	ud2
  6530fa:	mov    $0x3,%r15d
  653100:	mov    $0x3,%esi
  653105:	lea    0x82c5c(%rip),%rdx        # 6d5d68 <tokio::runtime::task::waker::WAKER_VTABLE+0x2a90>
  65310c:	jmp    652e9d <valar_spiral_rs::poly::from_ntt+0x122d>
  653111:	mov    0xe0(%rsp),%rsi
  653119:	lea    0x82c00(%rip),%rdx        # 6d5d20 <tokio::runtime::task::waker::WAKER_VTABLE+0x2a48>
  653120:	jmp    652e9d <valar_spiral_rs::poly::from_ntt+0x122d>
  653125:	mov    $0x4,%esi
  65312a:	lea    0x826e7(%rip),%rdx        # 6d5818 <tokio::runtime::task::waker::WAKER_VTABLE+0x2540>
  653131:	jmp    652e9d <valar_spiral_rs::poly::from_ntt+0x122d>
  653136:	mov    %rdi,%rsi
  653139:	xor    %r15d,%r15d
  65313c:	lea    0x82bdd(%rip),%rdx        # 6d5d20 <tokio::runtime::task::waker::WAKER_VTABLE+0x2a48>
  653143:	jmp    652e9d <valar_spiral_rs::poly::from_ntt+0x122d>
  653148:	mov    %rcx,%rsi
  65314b:	mov    $0x2,%r15d
  653151:	lea    0x82be0(%rip),%rdx        # 6d5d38 <tokio::runtime::task::waker::WAKER_VTABLE+0x2a60>
  653158:	jmp    652e9d <valar_spiral_rs::poly::from_ntt+0x122d>
  65315d:	incq   0x0(%rbp)
  653161:	mov    %rax,%rdi
  653164:	call   6a4f10 <_Unwind_Resume@plt>
  653169:	incq   0x0(%rbp)
  65316d:	mov    %rax,%rdi
  653170:	call   6a4f10 <_Unwind_Resume@plt>
